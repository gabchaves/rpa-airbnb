import os
import random
import re
import time
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, List, Set, Optional
from urllib.parse import quote

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

DATA_DIR = Path("data")
DEBUG_DIR = Path("debug")

DEFAULT_TARGET_COUNT = 10
DEFAULT_CITY = "Jacarei - SP"
CHECKPOINT_SIZE = 5

LogCallback = Callable[[str], None] | None


def log(message: str, callback: LogCallback = None) -> None:
    timestamp = time.strftime("%H:%M:%S")
    line = f"[{timestamp}] {message}"
    if callback:
        callback(line)
    print(line)


def normalize_city(city: str) -> str:
    value = (city or "").strip()
    return value or DEFAULT_CITY


def city_slug(city: str) -> str:
    normalized = unicodedata.normalize("NFKD", normalize_city(city))
    ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", ascii_only.lower()).strip("-")
    return slug or "cidade"


def build_search_url(city: str) -> str:
    location = quote(normalize_city(city))
    return f"https://www.airbnb.com.br/s/{location}/homes"


@dataclass
class ScraperPaths:
    output_file: Path
    discovered_urls_file: Path
    debug_screenshot_file: Path


def build_paths(city: str, output_folder: str = "") -> ScraperPaths:
    slug = city_slug(city)
    
    # Se pasta de saida foi definida, usa ela. Senao usa DATA_DIR
    base_out = Path(output_folder) if output_folder else DATA_DIR
    base_out.mkdir(parents=True, exist_ok=True)
    
    # Mantem caches internos na pasta do app sempre, pra nao sujar a pasta do usuario
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)

    return ScraperPaths(
        output_file=base_out / f"perfis_airbnb_{slug}.xlsx",
        discovered_urls_file=DATA_DIR / f"urls_descobertas_{slug}.txt",
        debug_screenshot_file=DEBUG_DIR / f"erro_busca_{slug}.png",
    )


def load_discovered_urls(path: Path) -> Set[str]:
    if not path.exists():
        return set()
    try:
        return {line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()}
    except:
        return set()


def save_discovered_urls(path: Path, urls: Set[str]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    ordered = sorted(urls)
    path.write_text("\n".join(ordered), encoding="utf-8")


def load_existing_dataframe(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_excel(path)
    except Exception:
        return pd.DataFrame()


def load_processed_urls(path: Path) -> Set[str]:
    df = load_existing_dataframe(path)
    if df.empty or "source_url" not in df.columns:
        return set()
    return set(df["source_url"].astype(str).tolist())


def parse_listings_count(text: str) -> int:
    """
    Refinado para evitar falsos positivos (como pegar '11' de datas ou precos).
    """
    if not text:
        return 0
        
    normalized = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii").lower()
    
    # Padroes estritos: OBRIGATORIO ter 'listing', 'anuncio', 'acomodacao', 'item'
    patterns = [
        r"(?:de|of)\s+([\d\.\,]+)\s+(?:itens|items|listings|anuncios|acomodacoes)",
        r"mostrando.*?([\d\.\,]+)\s+(?:itens|items)",
        r"([\d\.\,]+)\s+(?:anuncios|listings|acomodacoes|places)",
        r"(\d+)\s+(?:listings|anuncios)"
    ]
    
    for pattern in patterns:
        match = re.search(pattern, normalized, re.IGNORECASE)
        if match:
            val_str = re.sub(r"[^\d]", "", match.group(1))
            if val_str.isdigit():
                return int(val_str)
    
    # Se nao achou padrao explicito, retorna 0 para evitar chutar "11" de "Nov 11"
    return 0


class SeleniumScraper:
    def __init__(self, city: str, target_count: int, headless: bool, log_callback: LogCallback = None, output_folder: str = ""):
        self.city = normalize_city(city)
        self.target_count = max(1, int(target_count))
        self.headless = headless
        self.log_callback = log_callback
        self.output_folder = output_folder
        self.search_url = build_search_url(self.city)
        self.paths = build_paths(self.city, output_folder=self.output_folder)

        # Tenta carregar existente se houver no destino
        self.output_df = load_existing_dataframe(self.paths.output_file)
        
        # Cache de URLs processadas sempre fica na pasta do app para controle interno
        self.processed_urls = load_processed_urls(self.paths.output_file)
        self.discovered_urls = load_discovered_urls(self.paths.discovered_urls_file)
        
        self.driver = None

    def setup_driver(self):
        log("Configurando Driver Selenium (High Speed)...", self.log_callback)
        options = ChromeOptions()
        
        # OTIMIZACAO 1: Eager Loading (Nao espera assets pesados entrarem)
        options.page_load_strategy = 'eager'
        
        if self.headless:
            options.add_argument("--headless=new")
        
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1366,768")
        options.add_argument("--log-level=3")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

        # OTIMIZACAO 2: Bloquear imagens (Economiza muita banda/tempo)
        prefs = {"profile.managed_default_content_settings.images": 2}
        options.add_experimental_option("prefs", prefs)

        # Evita deteccao basica
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        service = ChromeService(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)
        self.driver.set_page_load_timeout(30) # Timeout menor ja que estamos em eager

    def close_driver(self):
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
            self.driver = None

    def flush_buffer(self, rows: List[dict]) -> None:
        if not rows:
            return

        df_new = pd.DataFrame(rows)
        # Recarrega em memoria caso tenha mudado
        if os.path.exists(self.paths.output_file):
            try:
                current_df = pd.read_excel(self.paths.output_file)
                self.output_df = pd.concat([current_df, df_new], ignore_index=True)
            except:
                pass
        else:
             if self.output_df.empty:
                self.output_df = df_new
             else:
                self.output_df = pd.concat([self.output_df, df_new], ignore_index=True)

        self.output_df.drop_duplicates(subset="source_url", keep="last", inplace=True)
        
        # Garante pasta de saida
        self.paths.output_file.parent.mkdir(parents=True, exist_ok=True)
        
        self.output_df.to_excel(self.paths.output_file, index=False)
        log(f"Checkpoint salvo em: {self.paths.output_file.name}", self.log_callback)

    def run(self) -> None:
        try:
            self.setup_driver()
            
            pending_urls = list(self.discovered_urls - self.processed_urls)

            log(f"Cidade: {self.city}", self.log_callback)
            
            # Se precisamos de mais URLs, tenta descobrir
            if len(pending_urls) < self.target_count:
                need = self.target_count - len(pending_urls)
                max_new = max(need + 5, 20)
                new_urls = self.discover_listings(max_new=max_new)
                
                if new_urls:
                    self.discovered_urls.update(new_urls)
                    save_discovered_urls(self.paths.discovered_urls_file, self.discovered_urls)
                    pending_urls = list(self.discovered_urls - self.processed_urls)
                    log(f"Novas URLs descobertas: {len(new_urls)}", self.log_callback)
                else:
                    log("Nenhuma nova URL encontrada na busca.", self.log_callback)

            urls_to_scrape = pending_urls[: self.target_count]
            if not urls_to_scrape:
                log("Sem URLs pendentes para processar.", self.log_callback)
                return

            log(f"Iniciando raspagem de {len(urls_to_scrape)} perfis...", self.log_callback)
            buffer: List[dict] = []

            for index, url in enumerate(urls_to_scrape, start=1):
                log(f"[{index}/{len(urls_to_scrape)}] Acessando: {url}", self.log_callback)
                
                data = self.scrape_profile(url)
                
                if data and data['host_profile_url']:
                    buffer.append(data) # Adiciona ao buffer local
                    self.processed_urls.add(url)
                    log(f"Sucesso: {data['host_name']} ({data['host_listings_count']} anuncios)", self.log_callback)
                else:
                    log("Falha ou dados incompletos.", self.log_callback)
                
                # Salva checkpoint
                if len(buffer) >= CHECKPOINT_SIZE:
                    self.flush_buffer(buffer)
                    buffer.clear()

                # OTIMIZACAO 3: Menos tempo ocioso
                time.sleep(random.uniform(0.5, 1.5))

            if buffer:
                self.flush_buffer(buffer)

            log(f"Finalizado! Arquivo salvo em:\n{self.paths.output_file}", self.log_callback)
            
        except Exception as e:
            log(f"Erro fatal no processo: {e}", self.log_callback)
        finally:
            self.close_driver()

    def discover_listings(self, max_new: int = 20) -> Set[str]:
        new_urls = set()
        page_number = 1
        
        log(f"Buscando listagens na pagina de busca...", self.log_callback)
        try:
            self.driver.get(self.search_url)
            # Removemos sleeps longos fixos
            
            while len(new_urls) < max_new:
                log(f"Varrendo pagina de busca {page_number}...", self.log_callback)
                
                # Scroll mais rapido (menos iteracoes, maior salto)
                for _ in range(3):
                    self.driver.execute_script("window.scrollBy(0, 1500);")
                    time.sleep(0.5)

                # Busca links padrao do Airbnb (/rooms/...)
                # Seletor generico mas confiavel
                elements = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='/rooms/']")
                before = len(new_urls)
                
                for el in elements:
                    try:
                        href = el.get_attribute("href")
                        if href and "/rooms/" in href:
                            clean = href.split("?")[0]
                            if clean not in self.discovered_urls and clean not in new_urls:
                                 new_urls.add(clean)
                    except:
                        continue
                
                found_now = len(new_urls) - before
                log(f"Links nesta pagina: +{found_now}", self.log_callback)
                
                if len(new_urls) >= max_new:
                    break
                
                # Tentar ir para proxima pagina
                try:
                    # Tenta achar o botao rapidamente
                    # Seletor amplo para pegar botao next em pt, en, es
                    next_btns = self.driver.find_elements(By.CSS_SELECTOR, "a[aria-label*='Proximo'], a[aria-label*='Next'], a[aria-label*='Siguiente']")
                    clicked = False
                    for btn in next_btns:
                        if btn.is_displayed() and btn.is_enabled():
                            try:
                                self.driver.execute_script("arguments[0].click();", btn)
                                clicked = True
                                break
                            except:
                                pass
                    
                    if clicked:
                        time.sleep(3) # Unico sleep mais longo necessario p/ troca de pagina
                        page_number += 1
                    else:
                        break
                except:
                    log("Fim das paginas de busca ou botao nao encontrado.", self.log_callback)
                    break
                    
        except Exception as e:
            log(f"Erro na descoberta: {e}", self.log_callback)
            
        return new_urls

    def scrape_profile(self, listing_url: str) -> Optional[dict]:
        try:
            self.driver.get(listing_url)
            
            # --- 1. PEGAR TITULO (Refinado) ---
            listing_title = "Titulo nao capturado"
            xpath_title_user = "/html/body/div[5]/div/div/div[1]/div/div/div[1]/div[2]/div/div/div/div[1]/div[2]/div[1]/div[1]/div[3]/div/div/div/div/div/section/div/div/div/h1"
            
            try:
                # Tenta XPath exato primeiro
                title_el = self.driver.find_element(By.XPATH, xpath_title_user)
                listing_title = title_el.text.strip()
            except:
                try:
                    # Fallback H1 generico
                    listing_title = self.driver.find_element(By.TAG_NAME, "h1").text.strip()
                except:
                    pass

            # --- 2. FECHAR MODAIS (Importante para clicar) ---
            # OTIMIZACAO: JS direto, mais rapido que find_element + click
            self.driver.execute_script("""
                const btn = document.querySelector("button[aria-label='Fechar']");
                if(btn) btn.click();
            """)
            
            self.driver.execute_script("window.scrollBy(0, 800);")
            time.sleep(0.5)

            # --- 3. LOCALIZAR LINK DO PERFIL (XPATH DO USUARIO) ---
            host_profile_url = None
            
            # XPATHS DE BUSCA
            # 1. O XPath exato que voce mandou
            xpath_user = "/html/body/div[5]/div/div/div[1]/div/div/div[1]/div[2]/div/div/div/div[1]/div[2]/div[1]/div[1]/div[14]/div/div/div/div[2]/section/div/div/div[2]/div[1]/div[2]/a"
            # 2. Fallback baseado no aria-label (da sua imagem)
            xpath_aria = "//a[@aria-label='Acessar o perfil completo do anfitrião']"

            target_el = None
            
            # log("Buscando link...", self.log_callback)
            try:
                # Tenta esperar o aria-label primeiro (mais confiavel visualmente)
                target_el = WebDriverWait(self.driver, 4).until(
                    EC.presence_of_element_located((By.XPATH, xpath_aria))
                )
            except:
                try:
                    # Fallback pro XPath absoluto
                    target_el = self.driver.find_element(By.XPATH, xpath_user)
                except:
                    pass

            if target_el:
                # OTIMIZACAO: Tentar pegar href direto primeiro sem scroll/clique se possivel
                host_profile_url = target_el.get_attribute("href")
                
                if not host_profile_url:
                    # Se falhar href, ai sim fazemos a interacao pesada
                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", target_el)
                    try:
                        self.driver.execute_script("arguments[0].click();", target_el)
                    except:
                        target_el.click()
                    
                    # Espera url mudar
                    try:
                        WebDriverWait(self.driver, 5).until(lambda d: "/users/" in d.current_url)
                        host_profile_url = self.driver.current_url
                    except:
                        pass
                else:
                    # Navegacao direta e mais rapida
                    self.driver.get(host_profile_url)

            if not host_profile_url:
                # Tenta fallback para link direto se nao achou o botao
                try: 
                   fallback_el = self.driver.find_element(By.XPATH, "//a[contains(@href, '/users/show/')]")
                   host_profile_url = fallback_el.get_attribute("href")
                   self.driver.get(host_profile_url)
                except:
                   return None

            if not host_profile_url:
                return None
            
            if "?" in host_profile_url:
                host_profile_url = host_profile_url.split("?")[0]

            # --- 4. EXTRAIR DADOS DO PERFIL ---
            
            host_name = "Nao encontrado"
            listings_count = 0
            
            # --- Nome ---
            xpath_name = '//*[@id="listings-scroller-heading"]/span'
            try:
                # Espera reduzida
                name_el = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, xpath_name))
                )
                host_name = name_el.text.strip()
                host_name = re.sub(r"^(Acomodações de|Listings by|Accommodations by)\s+", "", host_name, flags=re.IGNORECASE).strip()
            except Exception:
                 # Fallback pro H1
                try:
                    host_name = self.driver.find_element(By.TAG_NAME, "h1").text.replace("Sobre ", "").strip()
                except:
                    pass

            # --- Contagem ---
            xpath_count = '//*[@id="listings-scroller-description"]'
            try:
                count_el = self.driver.find_element(By.XPATH, xpath_count)
                count_text = count_el.text 
                listings_count = parse_listings_count(count_text)
            except Exception:
                pass

            # Se a contagem for 0, mas achamos o h1 do perfil, assume pelo menos 1
            if listings_count == 0 and host_name != "Nao encontrado":
                 listings_count = 1

            return {
                "city": self.city,
                "listing_title": listing_title,
                "host_name": host_name,
                "host_profile_url": host_profile_url,
                "host_listings_count": listings_count,
                "source_url": listing_url,
                "scraped_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            }

        except Exception as e:
            log(f"Erro URL: {str(e)[:50]}...", self.log_callback) # Log mais curto
            return None


def run_scraper(
    target_count: int = DEFAULT_TARGET_COUNT,
    city: str = DEFAULT_CITY,
    headless: bool = True,
    log_callback: LogCallback = None,
    output_folder: str = ""
) -> None:
    scraper = SeleniumScraper(city=city, target_count=target_count, headless=headless, log_callback=log_callback, output_folder=output_folder)
    scraper.run()


if __name__ == "__main__":
    try:
        count_input = input(f"Quantos perfis deseja raspar? (Enter para {DEFAULT_TARGET_COUNT}): ").strip()
        target = int(count_input) if count_input else DEFAULT_TARGET_COUNT
    except ValueError:
        target = DEFAULT_TARGET_COUNT

    city_input = input(f"Cidade para busca (Enter para '{DEFAULT_CITY}'): ").strip()
    city = city_input or DEFAULT_CITY
    # Forca visible mode para debug se rodar direto
    run_scraper(target_count=target, city=city, headless=False)

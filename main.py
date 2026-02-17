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


def build_paths(city: str) -> ScraperPaths:
    slug = city_slug(city)
    return ScraperPaths(
        output_file=DATA_DIR / f"perfis_airbnb_{slug}.xlsx",
        discovered_urls_file=DATA_DIR / f"urls_descobertas_{slug}.txt",
        debug_screenshot_file=DEBUG_DIR / f"erro_busca_{slug}.png",
    )


def load_discovered_urls(path: Path) -> Set[str]:
    if not path.exists():
        return set()
    return {line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()}


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
    Tenta extrair o numero total de anuncios a partir de textos como:
    'Mostrando x de y itens', 'Show x of y items' ou apenas 'y anuncios'.
    """
    if not text:
        return 0
        
    normalized = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii").lower()
    
    # Padroes comuns
    patterns = [
        r"(?:de|of)\s+([\d\.\,]+)\s+(?:itens|items|listings|anuncios|acomodacoes)",
        r"mostrando.*?([\d\.\,]+)\s+(?:itens|items)",
        r"([\d\.\,]+)\s+(?:anuncios|listings|acomodacoes)"
    ]
    
    for pattern in patterns:
        match = re.search(pattern, normalized, re.IGNORECASE)
        if match:
            # Remove pontos e virgulas e converte
            val_str = re.sub(r"[^\d]", "", match.group(1))
            if val_str.isdigit():
                return int(val_str)
                
    # Fallback: apenas numeros no texto
    digits_only = re.sub(r"[^\d]", "", normalized)
    if digits_only and len(digits_only) < 5: 
        return int(digits_only)
        
    return 0


class SeleniumScraper:
    def __init__(self, city: str, target_count: int, headless: bool, log_callback: LogCallback = None):
        self.city = normalize_city(city)
        self.target_count = max(1, int(target_count))
        self.headless = headless
        self.log_callback = log_callback
        self.search_url = build_search_url(self.city)
        self.paths = build_paths(self.city)

        self.output_df = load_existing_dataframe(self.paths.output_file)
        self.processed_urls = load_processed_urls(self.paths.output_file)
        self.discovered_urls = load_discovered_urls(self.paths.discovered_urls_file)
        
        self.driver = None

    def setup_driver(self):
        log("Configurando Driver Selenium...", self.log_callback)
        options = ChromeOptions()
        if self.headless:
            # options.add_argument("--headless=new") # Comentado por seguranca
            pass
        
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1366,768")
        options.add_argument("--log-level=3")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

        # Evita deteccao basica
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        service = ChromeService(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)
        self.driver.set_page_load_timeout(60)

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
        if self.output_df.empty:
            self.output_df = df_new
        else:
            self.output_df = pd.concat([self.output_df, df_new], ignore_index=True)

        self.output_df.drop_duplicates(subset="source_url", keep="last", inplace=True)
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.output_df.to_excel(self.paths.output_file, index=False)
        log(f"Checkpoint salvo ({len(self.output_df)} registros).", self.log_callback)

    def run(self) -> None:
        try:
            self.setup_driver()
            
            pending_urls = list(self.discovered_urls - self.processed_urls)

            log(f"Cidade: {self.city}", self.log_callback)
            log(f"URL de busca: {self.search_url}", self.log_callback)
            log(f"Ja processadas: {len(self.processed_urls)}", self.log_callback)
            log(f"URLs conhecidas: {len(self.discovered_urls)}", self.log_callback)
            
            # Se precisamos de mais URLs, tenta descobrir
            if len(pending_urls) < self.target_count:
                need = self.target_count - len(pending_urls)
                max_new = max(need + 5, 10)
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
                    buffer.append(data)
                    self.processed_urls.add(url)
                    log(f"Sucesso: {data['host_name']} ({data['host_listings_count']} anuncios)", self.log_callback)
                else:
                    log("Falha ou dados completados no fallback.", self.log_callback)
                
                # Salva checkpoint
                if len(buffer) >= CHECKPOINT_SIZE:
                    self.flush_buffer(buffer)
                    buffer.clear()

                time.sleep(random.uniform(2.0, 4.0))

            if buffer:
                self.flush_buffer(buffer)

            log(f"Finalizado. Arquivo salvo em: {self.paths.output_file}", self.log_callback)
            
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
            time.sleep(5) 
            
            while len(new_urls) < max_new:
                log(f"Varrendo pagina de busca {page_number}...", self.log_callback)
                
                # Scroll para carregar itens (lazy load)
                for _ in range(5):
                    self.driver.execute_script("window.scrollBy(0, 1000);")
                    time.sleep(1.0)

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
                    next_btn = self.driver.find_element(By.CSS_SELECTOR, "a[aria-label*='Proximo'], a[aria-label*='Next']")
                    if next_btn.is_enabled():
                        next_btn.click()
                        time.sleep(5) # Espera carregar nova pagina
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
            
            # --- 1. PEGAR TITULO (Opcional) ---
            listing_title = "Titulo nao capturado"
            try:
                WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.TAG_NAME, "h1")))
                listing_title = self.driver.find_element(By.TAG_NAME, "h1").text
            except:
                pass

            # --- 2. FECHAR MODAIS (Importante para clicar) ---
            try:
                close_btn = self.driver.find_element(By.CSS_SELECTOR, "button[aria-label='Fechar']")
                close_btn.click()
                time.sleep(1)
            except:
                pass
            
            # Rolar um pouco para garantir que elementos aparecam
            for _ in range(3):
                self.driver.execute_script("window.scrollBy(0, 700);")
                time.sleep(0.5)

            # --- 3. LOCALIZAR LINK DO PERFIL (XPATH DO USUARIO) ---
            host_profile_url = None
            
            # XPATHS DE BUSCA
            # 1. O XPath exato que voce mandou
            xpath_user = "/html/body/div[5]/div/div/div[1]/div/div/div[1]/div[2]/div/div/div/div[1]/div[2]/div[1]/div[1]/div[14]/div/div/div/div[2]/section/div/div/div[2]/div[1]/div[2]/a"
            
            # 2. Fallback baseado no aria-label (da sua imagem)
            xpath_aria = "//a[@aria-label='Acessar o perfil completo do anfitrião']"

            target_el = None
            
            log("Buscando link do perfil...", self.log_callback)
            
            # Tentativa 1: Aria Label (Mais robusto)
            try:
                target_el = self.driver.find_element(By.XPATH, xpath_aria)
                log("Perfil encontrado pelo aria-label (imagem).", self.log_callback)
            except:
                # Tentativa 2: XPath absoluto do usuario
                try:
                    target_el = self.driver.find_element(By.XPATH, xpath_user)
                    log("Perfil encontrado pelo XPath absoluto.", self.log_callback)
                except:
                    pass

            if target_el:
                # Rola ate ele
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", target_el)
                time.sleep(1)
                
                # Tenta pegar href
                host_profile_url = target_el.get_attribute("href")
                
                # Se nao tiver href ou for vazio, clica
                if not host_profile_url:
                    try:
                        target_el.click()
                    except:
                        self.driver.execute_script("arguments[0].click();", target_el)
                    
                    time.sleep(3)
                    host_profile_url = self.driver.current_url
                else:
                    self.driver.get(host_profile_url)
                    time.sleep(3)

            if not host_profile_url:
                log("ATENCAO: URL do perfil nao encontrada. Pulando...", self.log_callback)
                return None
            
            # Limpa URL se vier suja
            if "?" in host_profile_url:
                host_profile_url = host_profile_url.split("?")[0]

            # --- 4. EXTRAIR DADOS DO PERFIL ---
            log("Extraindo dados do perfil...", self.log_callback)
            
            host_name = "Nao encontrado"
            listings_count = 0
            
            # --- Nome: //*[@id="listings-scroller-heading"]/span ---
            xpath_name = '//*[@id="listings-scroller-heading"]/span'
            try:
                name_el = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, xpath_name))
                )
                host_name = name_el.text.strip()
            except Exception:
                # Fallback simples pro H1
                try:
                    host_name = self.driver.find_element(By.TAG_NAME, "h1").text.replace("Sobre ", "").strip()
                except:
                    pass

            # --- Contagem: //*[@id="listings-scroller-description"] ---
            xpath_count = '//*[@id="listings-scroller-description"]'
            try:
                count_el = self.driver.find_element(By.XPATH, xpath_count)
                count_text = count_el.text # "Mostrando x de y itens"
                listings_count = parse_listings_count(count_text)
            except Exception:
                pass

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
            log(f"Erro: {e}", self.log_callback)
            return None


def run_scraper(
    target_count: int = DEFAULT_TARGET_COUNT,
    city: str = DEFAULT_CITY,
    headless: bool = True,
    log_callback: LogCallback = None,
) -> None:
    scraper = SeleniumScraper(city=city, target_count=target_count, headless=headless, log_callback=log_callback)
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

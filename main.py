from playwright.sync_api import sync_playwright
import pandas as pd
import time
import os
import random

# Configurações
BATCH_SIZE = 10  # Quantos perfis raspar por vez
DATA_DIR = "data"
OUTPUT_FILE = os.path.join(DATA_DIR, "perfis_airbnb_jacarei.xlsx")
DISCOVERED_URLS_FILE = os.path.join(DATA_DIR, "todas_urls_encontradas.txt")
SEARCH_URL = "https://www.airbnb.com.br/s/Jacareí-~-SP/homes"

def load_processed_urls():
    if os.path.exists(OUTPUT_FILE):
        try:
            df = pd.read_excel(OUTPUT_FILE)
            if 'source_url' in df.columns:
                return set(df['source_url'].astype(str).tolist())
        except Exception:
            pass
    return set()

def load_discovered_urls():
    if os.path.exists(DISCOVERED_URLS_FILE):
        with open(DISCOVERED_URLS_FILE, "r") as f:
            return set(line.strip() for line in f if line.strip())
    return set()

def save_discovered_urls(urls):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(DISCOVERED_URLS_FILE, "w") as f:
        for url in urls:
            f.write(f"{url}\n")

def discover_listings(page, existing_discovered_urls, max_new=20):
    print("\n--- Buscando novos anúncios na pesquisa ---")
    new_urls_found = set()
    
    try:
        page.goto(SEARCH_URL, timeout=60000)
        # Tenta esperar por algo que indique carregamento de conteúdo
        try:
            page.wait_for_selector('div[role="main"]', timeout=30000)
        except:
            print("  -> Aviso: div[role='main'] demorou. Tentando body...")
            page.wait_for_selector('body', timeout=30000)

        # Verifica se apareceu Captcha ou Bloqueio
        if "captcha" in page.url or "challenge" in page.title().lower():
             print("  -> ALERTA: Possível Captcha/Bloqueio detectado!")
             page.screenshot(path="debug/bloqueio_detectado.png")
             input("Pressione Enter no terminal após resolver o Captcha manualmente...")
    
        page_num = 1
        
        while len(new_urls_found) < max_new:
            print(f"Varrendo página {page_num} da busca...")
            
            # Rola para carregar lazy loading
            for _ in range(5):
                page.mouse.wheel(0, 1000)
                time.sleep(1)
            
            # Extrai links
            # Tenta seletores mais amplos se o específico falhar
            links = page.locator('a[href^="/rooms/"]').all()
            if not links:
                 # Tenta buscar qualquer link que pareça um anúncio
                 links = page.locator('a[href*="/rooms/"]').all()

            count_before = len(new_urls_found)
            
            for link in links:
                href = link.get_attribute("href")
                if href:
                    # Limpa a URL (remove query params extra sse houver, mantém o ID)
                    clean_url = "https://www.airbnb.com.br" + href.split('?')[0]
                    if clean_url not in existing_discovered_urls and clean_url not in new_urls_found:
                        new_urls_found.add(clean_url)
            
            print(f"  -> Encontrados nesta página: {len(new_urls_found) - count_before} novos anúncios.")
            
            if len(new_urls_found) >= max_new:
                break
                
            # Tenta ir para próxima página
            try:
                # Seletor do botão próximo pode variar
                next_button = page.locator('a[aria-label="Próximo"]')
                if not next_button.is_visible():
                     next_button = page.locator('nav').locator('a', has_text="Próximo") # Tenta achar pelo texto dentro de nav
                
                if next_button.is_visible() and next_button.is_enabled():
                    print("  -> Indo para próxima página...")
                    next_button.click()
                    # Espera a URL mudar ou conteúdo recarregar
                    page.wait_for_load_state("networkidle", timeout=30000)
                    time.sleep(3)
                    page_num += 1
                else:
                    print("  -> Fim das páginas de busca ou botão Próximo não encontrado.")
                    break
            except Exception as e_nav:
                print(f"  -> Erro ao navegar para próxima página: {e_nav}")
                break
                
        return new_urls_found

    except Exception as e:
        print(f"Erro na busca: {e}")
        os.makedirs("debug", exist_ok=True)
        page.screenshot(path="debug/erro_busca_listing.png")
        # Retorna o que achou até o momento do erro
        return new_urls_found

def scrape_profile(page, url):
    print(f"Acessando: {url}")
    try:
        page.goto(url, timeout=60000)
        page.wait_for_selector('h1', timeout=20000)
        
        try:
            page.locator("button[aria-label='Fechar']").click(timeout=2000)
        except:
            pass

        try:
            listing_title = page.evaluate("document.querySelector('h1').innerText")
        except:
            listing_title = "Título não encontrado"

        # Busca link de perfil
        print("  -> Procurando link do anfitrião...")
        
        # Rola para baixo
        page.mouse.wheel(0, 3000)
        time.sleep(1)
        page.mouse.wheel(0, 3000)
        time.sleep(1)
        
        host_name = "Não encontrado"
        host_profile_url = "Não encontrado"
        listings_count = 0
        
        selectors_to_try = [
            "a[aria-label*='anfitrião']",
            "a[href*='/users/show/']",
            "a[href*='/users/profile/']",
            "div[class*='_'] > a[href^='/users/']"
        ]
        
        profile_element = None
        for selector in selectors_to_try:
            if page.locator(selector).count() > 0:
                profile_element = page.locator(selector).first
                break
        
        if profile_element:
            href = profile_element.get_attribute("href")
            host_profile_url = "https://www.airbnb.com.br" + href if href.startswith("/") else href
            
            print(f"  -> Indo para perfil: {host_profile_url}")
            page.goto(host_profile_url, timeout=60000)
            page.wait_for_load_state("domcontentloaded")
            time.sleep(3)

            try:
                raw_name = page.locator("h1").first.inner_text()
                host_name = raw_name.replace("Sobre ", "").strip()
            except:
                pass

            # Contagem
            print("  -> Contando acomodações...")
            try:
                listings_section = page.locator("h2", has_text="Acomodações de").first
                if not listings_section.is_visible():
                        listings_section = page.locator("section", has_text="Acomodações de").first
                
                if listings_section.is_visible():
                    listings_section.scroll_into_view_if_needed()
                    time.sleep(1.5)
                    
                    # Tenta contar cards
                    cards = page.locator('section').filter(has_text="Acomodações de").locator('a[href*="/rooms/"]').count()
                    
                    if cards > 0:
                        listings_count = cards
                    else:
                        text_counter = page.locator("div", has_text="Mostrando").filter(has_text="item").first
                        if text_counter.count() > 0:
                            text = text_counter.inner_text()
                            import re
                            match = re.search(r'de\s+(\d+)\s+item', text)
                            if match:
                                listings_count = int(match.group(1))
                else:
                    body_text = page.inner_text("body")
                    import re
                    match = re.search(r'(\d+)\s+(acomodaç|listing|anúncio)', body_text, re.IGNORECASE)
                    if match:
                        listings_count = int(match.group(1))

            except Exception:
                pass

        else:
            print("  -> Link de perfil NÃO encontrado.")

        return {
            "listing_title": listing_title,
            "host_name": host_name,
            "host_profile_url": host_profile_url,
            "host_listings_count": listings_count,
            "source_url": url,
            "scraped_at": time.strftime("%Y-%m-%d %H:%M:%S")
        }

    except Exception as e:
        print(f"  -> Erro ao processar URL: {e}")
        return None

def setup_context(browser):
    context = browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        viewport={'width': 1280, 'height': 720}
    )
    # Bloqueia carregamento de imagens e media para velocidade
    context.route("**/*.{png,jpg,jpeg,gif,webp,svg,mp4,woff,woff2}", lambda route: route.abort())
    return context

def main():
    processed_urls = load_processed_urls()
    discovered_urls = load_discovered_urls()
    
    # Identifica URLs que foram descobertas mas ainda não processadas
    pending_urls = list(discovered_urls - processed_urls)
    
    print(f"Status Atual:")
    print(f"- URLs Já Processadas: {len(processed_urls)}")
    print(f"- URLs Conhecidas (Total): {len(discovered_urls)}")
    print(f"- URLs Pendentes na Fila: {len(pending_urls)}")
    
    # Input do usuário para quantidade
    try:
        limit_input = input("\nQuantos perfis você quer raspar agora? (Enter para 10): ")
        target_count = int(limit_input) if limit_input.strip() else 10
    except ValueError:
        target_count = 10
    
    with sync_playwright() as p:
        # Modo rápido: headless=True (sem interface visual) e sem slow_mo
        # Se quiser ver rodando, mude para headless=False
        print("\nIniciando navegador otimizado (sem imagens/media)...")
        browser = p.chromium.launch(headless=True) 
        context = setup_context(browser)
        page = context.new_page()

        # SE PRECISAR DE MAIS URLS
        if len(pending_urls) < target_count:
            needed = target_count - len(pending_urls)
            print(f"\nFila baixa. Buscando pelo menos mais {needed} anúncios...")
            
            # Busca um pouco mais que o necessário para garantir
            new_urls = discover_listings(page, discovered_urls, max_new=max(needed + 10, 20))
            if new_urls:
                discovered_urls.update(new_urls)
                save_discovered_urls(discovered_urls)
                pending_urls = list(discovered_urls - processed_urls)
                print(f"Lista de URLs atualizada. Total agora: {len(discovered_urls)}")
            else:
                print("Nenhuma nova URL encontrada na busca.")

        # SELEÇÃO DO LOTE
        if not pending_urls:
            print("\nNão há URLs pendentes para processar.")
            browser.close()
            return

        urls_to_scrape = pending_urls[:target_count]
        print(f"\n--- Iniciando processamento de {len(urls_to_scrape)} URLs ---")
        
        new_data = []
        for i, url in enumerate(urls_to_scrape):
            print(f"[{i+1}/{len(urls_to_scrape)}] Processando...")
            data = scrape_profile(page, url)
            if data:
                new_data.append(data)
                print(f"  -> Sucesso: {data['host_name']} ({data['host_listings_count']} imóveis)")
            else:
                print("  -> Falha na extração.")
            
            # Pausa reduzida já que estamos bloqueando recursos e sem ver a tela
            # Mas ainda precisa ser humano para não tomar block instantâneo
            time.sleep(random.uniform(2, 4))
            
            # Salva parcial a cada 10 para não perder tudo se cair
            if len(new_data) % 10 == 0:
                 print("  -> Salvamento parcial...")
                 save_data(new_data) # Vamos extrair a função save para reutilizar

        browser.close()

        # Salva final
        if new_data:
            save_data(new_data)
        else:
            print("\nNenhum dado válido extraído neste lote.")

def save_data(new_data):
    if not new_data: return
    
    df_new = pd.DataFrame(new_data)
    
    if os.path.exists(OUTPUT_FILE):
        try:
            df_existing = pd.read_excel(OUTPUT_FILE)
            df_final = pd.concat([df_existing, df_new], ignore_index=True)
            # Remove duplicatas baseadas na URL, mantendo a última versão (novo scrape)
            df_final.drop_duplicates(subset='source_url', keep='last', inplace=True)
        except:
            df_final = df_new
    else:
        df_final = df_new
    
    df_final.to_excel(OUTPUT_FILE, index=False)
    print(f"Dados salvos em '{OUTPUT_FILE}' (Total: {len(df_final)})")

if __name__ == "__main__":
    main()

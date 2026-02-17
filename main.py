import os
import random
import re
import time
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Callable
from urllib.parse import quote

import pandas as pd
from playwright.sync_api import Page, TimeoutError, sync_playwright

DATA_DIR = Path("data")
DEBUG_DIR = Path("debug")

DEFAULT_TARGET_COUNT = 10
DEFAULT_CITY = "Jacarei - SP"
CHECKPOINT_SIZE = 10

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


def load_discovered_urls(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()}


def save_discovered_urls(path: Path, urls: set[str]) -> None:
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


def load_processed_urls(path: Path) -> set[str]:
    df = load_existing_dataframe(path)
    if df.empty or "source_url" not in df.columns:
        return set()
    return set(df["source_url"].astype(str).tolist())


def parse_listings_count(text: str) -> int:
    normalized = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii").lower()
    patterns = [
        r"mostrando\s+\d+\s*(?:-|a|ate)\s*\d+\s+de\s+(\d[\d\.\,]*)\s+itens?",
        r"showing\s+\d+\s*(?:-|to)\s*\d+\s+of\s+(\d[\d\.\,]*)\s+items?",
        r"(\d[\d\.\,]*)\s+(?:acomodacoes|acomodacao|listings|listing|anuncios|anuncio)",
        r"de\s+(\d[\d\.\,]*)\s+itens?",
        r"of\s+(\d[\d\.\,]*)\s+items?",
    ]
    for pattern in patterns:
        match = re.search(pattern, normalized, re.IGNORECASE)
        if not match:
            continue
        value = re.sub(r"\D", "", match.group(1))
        if value.isdigit():
            return int(value)
    return 0


def setup_context(browser):
    context = browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1366, "height": 768},
        locale="pt-BR",
        service_workers="block",
    )

    blocked_types = {"image", "media", "font"}

    def route_handler(route):
        if route.request.resource_type in blocked_types:
            route.abort()
        else:
            route.continue_()

    context.route("**/*", route_handler)
    return context


class AirbnbScraper:
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

    def run(self) -> None:
        pending_urls = list(self.discovered_urls - self.processed_urls)

        log(f"Cidade: {self.city}", self.log_callback)
        log(f"URL de busca: {self.search_url}", self.log_callback)
        log(f"Ja processadas: {len(self.processed_urls)}", self.log_callback)
        log(f"URLs conhecidas: {len(self.discovered_urls)}", self.log_callback)
        log(f"URLs pendentes: {len(pending_urls)}", self.log_callback)

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=self.headless)
            context = setup_context(browser)
            page = context.new_page()
            page.set_default_timeout(30000)

            if len(pending_urls) < self.target_count:
                need = self.target_count - len(pending_urls)
                max_new = max(need + 20, 30)
                new_urls = self.discover_listings(page, max_new=max_new)
                if new_urls:
                    self.discovered_urls.update(new_urls)
                    save_discovered_urls(self.paths.discovered_urls_file, self.discovered_urls)
                    pending_urls = list(self.discovered_urls - self.processed_urls)
                    log(f"Novas URLs descobertas: {len(new_urls)}", self.log_callback)
                    log(f"Fila atualizada: {len(pending_urls)} pendentes", self.log_callback)
                else:
                    log("Nenhuma nova URL encontrada na busca.", self.log_callback)

            urls_to_scrape = pending_urls[: self.target_count]
            if not urls_to_scrape:
                log("Sem URLs pendentes para processar.", self.log_callback)
                browser.close()
                return

            log(f"Iniciando lote com {len(urls_to_scrape)} URLs...", self.log_callback)
            buffer: list[dict] = []

            for index, url in enumerate(urls_to_scrape, start=1):
                log(f"[{index}/{len(urls_to_scrape)}] Processando {url}", self.log_callback)
                row = self.scrape_profile(page, url)
                if row:
                    buffer.append(row)
                    self.processed_urls.add(url)
                    log(
                        f"Sucesso: {row['host_name']} ({row['host_listings_count']} anuncios)",
                        self.log_callback,
                    )
                else:
                    log("Falha ao extrair dados desta URL.", self.log_callback)

                if len(buffer) >= CHECKPOINT_SIZE:
                    self.flush_buffer(buffer)
                    buffer.clear()

                time.sleep(random.uniform(1.0, 2.2))

            if buffer:
                self.flush_buffer(buffer)

            browser.close()
            log(f"Finalizado. Arquivo salvo em: {self.paths.output_file}", self.log_callback)

    def flush_buffer(self, rows: list[dict]) -> None:
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

    def discover_listings(self, page: Page, max_new: int = 30) -> set[str]:
        new_urls: set[str] = set()
        page_number = 1

        try:
            log("Buscando novas URLs de anuncios...", self.log_callback)
            page.goto(self.search_url, timeout=60000)
            page.wait_for_load_state("domcontentloaded")
            self.dismiss_modal(page)

            while len(new_urls) < max_new:
                log(f"Varrendo pagina {page_number}...", self.log_callback)
                self.scroll_page(page)

                found = page.eval_on_selector_all(
                    "a[href*='/rooms/']",
                    "els => els.map(e => e.href).filter(Boolean)",
                )
                before = len(new_urls)
                for href in found:
                    clean_url = href.split("?")[0]
                    if "/rooms/" not in clean_url:
                        continue
                    if clean_url in self.discovered_urls or clean_url in new_urls:
                        continue
                    new_urls.add(clean_url)

                log(f"Novas URLs nesta pagina: {len(new_urls) - before}", self.log_callback)
                if len(new_urls) >= max_new:
                    break

                if not self.go_to_next_page(page):
                    break
                page_number += 1

        except Exception as exc:
            log(f"Erro durante descoberta de URLs: {exc}", self.log_callback)
            DEBUG_DIR.mkdir(parents=True, exist_ok=True)
            page.screenshot(path=str(self.paths.debug_screenshot_file))

        return new_urls

    def scrape_profile(self, page: Page, listing_url: str) -> dict | None:
        try:
            page.goto(listing_url, timeout=60000)
            page.wait_for_selector("h1", timeout=25000)
            self.dismiss_modal(page)

            listing_title = page.locator("h1").first.inner_text().strip()
            self.scroll_page(page)
            host_url = self.open_host_profile(page)
            host_name = "Nao encontrado"
            listings_count = 0

            if host_url:
                log(f"Perfil encontrado: {host_url}", self.log_callback)
                self.dismiss_modal(page)
                time.sleep(1.5)
                self.scroll_page(page)

                if page.locator("h1").count() > 0:
                    host_name = page.locator("h1").first.inner_text().replace("Sobre ", "").strip()
                listings_count = self.extract_listings_count_from_profile(page)
            else:
                log("Nao foi possivel encontrar o link do perfil do anfitriao.", self.log_callback)

            return {
                "city": self.city,
                "listing_title": listing_title,
                "host_name": host_name,
                "host_profile_url": host_url or "Nao encontrado",
                "host_listings_count": listings_count,
                "source_url": listing_url,
                "scraped_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
        except Exception as exc:
            log(f"Erro ao raspar {listing_url}: {exc}", self.log_callback)
            return None

    @staticmethod
    def dismiss_modal(page: Page) -> None:
        selectors = [
            "button[aria-label='Fechar']",
            "button[aria-label='Close']",
            "button:has-text('Aceitar')",
            "button:has-text('Accept')",
        ]
        for selector in selectors:
            locator = page.locator(selector)
            if locator.count() > 0:
                try:
                    locator.first.click(timeout=1000)
                    return
                except Exception:
                    continue

    @staticmethod
    def scroll_page(page: Page) -> None:
        for _ in range(4):
            page.mouse.wheel(0, 1400)
            time.sleep(0.6)

    @staticmethod
    def extract_listings_count_from_profile(page: Page) -> int:
        # 1) Bloco principal do perfil.
        selectors = [
            "section:has-text('Acomodacoes de') a[href*='/rooms/']",
            "section:has-text('Listings by') a[href*='/rooms/']",
            "section:has-text('anuncio') a[href*='/rooms/']",
            "section:has-text('listing') a[href*='/rooms/']",
        ]
        for selector in selectors:
            try:
                count = page.locator(selector).count()
                if count > 0:
                    return count
            except Exception:
                continue

        # 2) Texto de paginacao/listagem com total real.
        try:
            body_text = page.inner_text("body")
            total = parse_listings_count(body_text)
            if total > 0:
                return total
        except Exception:
            pass

        # 3) Fallback: quantidade de links /rooms/ visiveis.
        try:
            hrefs = page.eval_on_selector_all(
                "a[href*='/rooms/']",
                "els => [...new Set(els.map(e => e.href.split('?')[0]).filter(h => h.includes('/rooms/')))]",
            )
            if hrefs:
                return len(hrefs)
        except Exception:
            pass

        # 4) Ultimo fallback por varredura geral.
        candidates: list[str] = []
        for selector in ["h2", "h3", "section", "div", "span"]:
            try:
                candidates.extend(page.locator(selector).all_inner_texts())
            except Exception:
                continue
        try:
            candidates.append(page.inner_text("body"))
        except Exception:
            pass
        return parse_listings_count("\n".join(candidates))

    @staticmethod
    def go_to_next_page(page: Page) -> bool:
        selectors = [
            "a[aria-label='Proximo']",
            "a[aria-label='Próximo']",
            "a[aria-label='Next']",
            "nav a:has-text('Proximo')",
            "nav a:has-text('Próximo')",
            "nav a:has-text('Next')",
        ]
        for selector in selectors:
            locator = page.locator(selector).first
            if locator.count() == 0:
                continue
            try:
                if locator.is_visible() and locator.is_enabled():
                    locator.click()
                    page.wait_for_load_state("domcontentloaded")
                    time.sleep(1.0)
                    return True
            except Exception:
                continue
        return False

    @staticmethod
    def find_host_profile_url(page: Page) -> str | None:
        selectors = [
            "a[aria-label*='anfitrião']",
            "a[aria-label*='anfitriao']",
            "a[aria-label*='host']",
            "a[href*='/users/show/']",
            "a[href*='/users/profile/']",
            "div[class*='_'] > a[href^='/users/']",
            "a[href^='/users/']",
        ]
        for selector in selectors:
            locator = page.locator(selector)
            if locator.count() == 0:
                continue
            href = locator.first.get_attribute("href")
            if not href:
                continue
            if href.startswith("/"):
                return f"https://www.airbnb.com.br{href.split('?')[0]}"
            return href.split("?")[0]

        # Fallback: procura direto no HTML quando o link nao esta visivel no DOM principal.
        html = page.content()
        patterns = [
            r"https://www\.airbnb\.com(?:\.br)?/users/show/\d+",
            r"https://www\.airbnb\.com(?:\.br)?/users/profile/\d+",
            r"(/users/show/\d+)",
            r"(/users/profile/\d+)",
        ]
        for pattern in patterns:
            match = re.search(pattern, html)
            if not match:
                continue
            raw = match.group(1) if match.lastindex else match.group(0)
            if raw.startswith("/"):
                return f"https://www.airbnb.com.br{raw}"
            return raw
        return None

    def open_host_profile(self, page: Page) -> str | None:
        selectors = [
            "a[aria-label*='anfitrião']",
            "a[aria-label*='anfitriao']",
            "a[aria-label*='host']",
            "a[href*='/users/show/']",
            "a[href*='/users/profile/']",
            "div[class*='_'] > a[href^='/users/']",
            "a[href^='/users/']",
        ]

        for selector in selectors:
            locator = page.locator(selector)
            if locator.count() == 0:
                continue
            try:
                locator.first.scroll_into_view_if_needed()
                with page.expect_navigation(timeout=15000):
                    locator.first.click()
                if "/users/" in page.url:
                    return page.url.split("?")[0]
            except TimeoutError:
                continue
            except Exception:
                continue

        host_url = self.find_host_profile_url(page)
        if not host_url:
            return None
        try:
            page.goto(host_url, timeout=60000)
            page.wait_for_load_state("domcontentloaded")
            return page.url.split("?")[0]
        except Exception:
            return host_url


def run_scraper(
    target_count: int = DEFAULT_TARGET_COUNT,
    city: str = DEFAULT_CITY,
    headless: bool = True,
    log_callback: LogCallback = None,
) -> None:
    scraper = AirbnbScraper(city=city, target_count=target_count, headless=headless, log_callback=log_callback)
    scraper.run()


if __name__ == "__main__":
    try:
        count_input = input(f"Quantos perfis deseja raspar? (Enter para {DEFAULT_TARGET_COUNT}): ").strip()
        target = int(count_input) if count_input else DEFAULT_TARGET_COUNT
    except ValueError:
        target = DEFAULT_TARGET_COUNT

    city_input = input(f"Cidade para busca (Enter para '{DEFAULT_CITY}'): ").strip()
    city = city_input or DEFAULT_CITY
    run_scraper(target_count=target, city=city, headless=True)

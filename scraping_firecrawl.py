import json
import os
import pandas as pd
from firecrawl import FirecrawlApp
from dotenv import load_dotenv

# Carrega variáveis de ambiente do arquivo .env
load_dotenv()

api_key = os.getenv("FIRECRAWL_API_KEY")
if not api_key:
    raise ValueError("A chave da API do Firecrawl não foi encontrada. Verifique o arquivo .env.")

app = FirecrawlApp(api_key=api_key)

def main():
    print("Pesquisando acomodações no Airbnb em Jacareí...")
    
    # 1. Pesquisa por anúncios no Airbnb em Jacareí
    search_query = "site:airbnb.com.br aluguel temporada Jacareí"
    try:
        # Tenta usar a API antiga (algumas versoes usam params, outras args diretos)
        # Se params falhar, tenta args diretos
        search_results = app.search(
            query=search_query,
            limit=5,
            scrape_options={
                "formats": ["markdown"]
            }
        )
    except TypeError:
         # Fallback para versoes antigas que usam params (embora o erro diga o contrario, vamos garantir)
         search_results = app.search(
            query=search_query,
            params={
                "limit": 5,
                "scrapeOptions": {
                    "formats": ["markdown"]
                }
            }
        )

    listings = []
    if 'data' in search_results:
        listings = search_results['data']
        print(f"Encontrados {len(listings)} resultados na pesquisa.")
    else:
        print("A pesquisa não retornou resultados diretos (pode ser um bloqueio temporário ou formato inesperado).")
        print(f"Debug - Resposta da pesquisa: {search_results}")

    profiles = []

    # Usamos URLs dos resultados da pesquisa se possível, ou URLs de exemplo para demonstração
    urls_to_scrape = [
        item.get('url') for item in listings if 'airbnb.com.br/rooms/' in item.get('url', '')
    ]
    
    # Se não encontrar URLs diretos nos resultados (o que pode acontecer dependendo do que o search retorna),
    # usamos uma lista de fallback ou tentamos extrair de outra forma.
    if not urls_to_scrape:
        print("Usando URLs de exemplo (fallback)...")
        urls_to_scrape = [
            "https://www.airbnb.com.br/rooms/1312580366220227562",
            "https://www.airbnb.com.br/rooms/1386575264174953616",
            "https://www.airbnb.com.br/rooms/1038436854836805051",
            "https://www.airbnb.com.br/rooms/1416115021183400732",
            "https://www.airbnb.com.br/rooms/963776201299188428"
        ]

    print(f"Iniciando extração individual de {len(urls_to_scrape)} URLs...")

    # Processa cada URL individualmente para garantir precisão
    for url in urls_to_scrape:
        print(f"Extraindo dados de: {url}")
        try:
            extraction = app.extract(
                [url], # Passa como lista de 1 item
                prompt="Extract the host name, their profile URL, and specifically look for the number of listings/accommodations this host has (host_listings_count). It is often near their profile picture or description, stated as 'X listings', 'X acomodações', or similar.",
                schema={
                    "type": "object",
                    "properties": {
                        "host_name": { "type": "string" },
                        "host_profile_url": { "type": "string" },
                        "listing_title": { "type": "string" },
                        "host_listings_count": { "type": "integer" },
                        "about_host": { "type": "string" }
                    },
                    "required": ["host_name"]
                }
            )
            
            if extraction.success:
                data = extraction.data
                if isinstance(data, dict):
                    # Adiciona o URL original para referência
                    data['source_url'] = url
                    profiles.append(data)
                    print(f"  -> Sucesso: {data.get('host_name')} ({data.get('host_listings_count', 0)} imóveis)")
                elif isinstance(data, list) and len(data) > 0:
                    item = data[0]
                    if hasattr(item, 'dict'):
                        item = item.dict()
                    item['source_url'] = url
                    profiles.append(item)
                    print(f"  -> Sucesso: {item.get('host_name')} ({item.get('host_listings_count', 0)} imóveis)")
            else:
                print(f"  -> Falha na extração: {extraction}")
                
        except Exception as e:
            print(f"  -> Erro ao processar URL: {e}")

    if profiles:
         print(f"\nExtração concluída! {len(profiles)} perfis coletados.")
         
         # Cria diretório data se não existir
         os.makedirs("data", exist_ok=True)
         
         # Salva como Excel usando Pandas
         df = pd.DataFrame(profiles)
         output_excel = os.path.join("data", "perfis_airbnb_jacarei.xlsx")
         df.to_excel(output_excel, index=False)
         print(f"Dados salvos em '{output_excel}'")
    else:
         print("Nenhum dado extraído.")

if __name__ == "__main__":
    main()

# Airbnb Host Scraper

Scraper de perfis de anfitrioes do Airbnb com:
- busca por cidade dinamica
- interface grafica para definir cidade e quantidade de perfis
- salvamento incremental em Excel por cidade

## Como executar

1. Instale dependencias:
```bash
uv sync
```

2. Rode a interface grafica:
```bash
uv run python gui.py
```

3. Opcional: rode via terminal:
```bash
uv run python main.py
```

## Saida

- Arquivos de dados em `data/perfis_airbnb_<cidade>.xlsx`
- URLs descobertas em `data/urls_descobertas_<cidade>.txt`
- Screenshots de erro em `debug/`

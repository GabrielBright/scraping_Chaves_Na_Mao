from playwright.async_api import async_playwright
import asyncio
import os
import pickle
import pandas as pd
from urllib.parse import urljoin
import traceback

ARQUIVO_PICKLE = "links_chaves_na_mao_motos.pkl"
ARQUIVO_EXCEL = "links_chaves_na_mao_motos.xlsx"
DOMINIO_BASE = "https://www.chavesnamao.com.br/"

# Lista de URLs para scraping
links = [
    ("Motos", "https://www.chavesnamao.com.br/motos-usadas/brasil/?filtro=amin:2002,amax:", 1700),
    ("Motos", "https://www.chavesnamao.com.br/motos-usadas/brasil/?&filtro=amin:2002,amax:0,or:4", 1700),
    ("Curitiba", "https://www.chavesnamao.com.br/motos-usadas/pr-curitiba/?filtro=amin:2002,amax:0", 350),
    ("RJ", "https://www.chavesnamao.com.br/motos-usadas/rj-rio-de-janeiro/?filtro=amin:2002,amax:0", 221),
    ("Honda", "https://www.chavesnamao.com.br/motos/brasil/honda/2002/", 1085),
    ("Yamaha", "https://www.chavesnamao.com.br/motos/brasil/yamaha/2002/", 476),
    ("BMW", "https://www.chavesnamao.com.br/motos/brasil/bmw/2002/", 210),
    ("Harley-davidson", "https://www.chavesnamao.com.br/motos/brasil/harley-davidson/2002/", 126),
    ("Kawasaki", "https://www.chavesnamao.com.br/motos/brasil/kawasaki/2002/", 109),
    ("Suzuki", "https://www.chavesnamao.com.br/motos/brasil/suzuki/2002/", 89),
    ("Triumph", "https://www.chavesnamao.com.br/motos/brasil/triumph/2002/", 78),
    ("Haojue", "https://www.chavesnamao.com.br/motos/brasil/haojue/2002/", 63),
    ("Ducati", "https://www.chavesnamao.com.br/motos/brasil/ducati/2002/", 41),
    ("Shineray", "https://www.chavesnamao.com.br/motos/brasil/shineray/2002/", 38),
    ("Royal Enfield", "https://www.chavesnamao.com.br/motos/brasil/royal-enfield/2002/", 23),
    ("Cfmoto", "https://www.chavesnamao.com.br/motos/brasil/cfmoto/2002/", 5),
]

def salvar_progresso(dados):
    if os.path.exists(ARQUIVO_PICKLE):
        with open(ARQUIVO_PICKLE, "rb") as f:
            dados_existentes = pickle.load(f)
    else:
        dados_existentes = set()

    novos_dados = {d["Link"] for d in dados}
    dados_existentes.update(novos_dados)

    with open(ARQUIVO_PICKLE, "wb") as f:
        pickle.dump(dados_existentes, f)
    print(f"Progresso salvo em '{ARQUIVO_PICKLE}'")

def carregar_progresso():
    if os.path.exists(ARQUIVO_PICKLE):
        with open(ARQUIVO_PICKLE, "rb") as f:
            return pickle.load(f)
    return set()

async def rolar_e_coletar(pagina, limite_itens):
    ids_itens_carregados = set()
    tentativas_sem_novos_itens = 0
    altura_anterior = 0

    while tentativas_sem_novos_itens < 5 and len(ids_itens_carregados) < limite_itens:
        try:
            await pagina.wait_for_selector('//a[@href]', timeout=30000)
            links = await pagina.query_selector_all('//a[@href]')
            novos_itens = 0

            for link in links:
                if len(ids_itens_carregados) >= limite_itens:
                    break
                href = await link.get_attribute('href')
                if href:
                    href = urljoin(DOMINIO_BASE, href)
                    if "/moto/" in href and "/id-" in href and href not in ids_itens_carregados:
                        ids_itens_carregados.add(href)
                        novos_itens += 1

            print(f"Total de links coletados até agora: {len(ids_itens_carregados)}")

            altura_atual = await pagina.evaluate("document.body.scrollHeight")
            if altura_atual == altura_anterior and novos_itens == 0:
                tentativas_sem_novos_itens += 1
            else:
                tentativas_sem_novos_itens = 0
                altura_anterior = altura_atual

            await pagina.evaluate("window.scrollBy(0, document.body.scrollHeight)")
            await asyncio.sleep(3)

        except Exception as e:
            print(f"Erro ao encontrar links: {e}")
            traceback.print_exc()
            tentativas_sem_novos_itens += 1
            await asyncio.sleep(3)

    print(f"Fim da rolagem para esta página. Total de itens carregados: {len(ids_itens_carregados)}")
    return [{"Link": link} for link in ids_itens_carregados]

async def processar_url(navegador, marca, url, limite, links_existentes):
    if url in links_existentes:
        print(f"{marca} já processado. Pulando...")
        return []

    print(f"Abrindo página para {marca} ({url})...")
    pagina = await navegador.new_page(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
    try:
        await pagina.route("**/*.{png,jpg,jpeg,gif,webp}", lambda route: route.abort())
        await pagina.goto(url, timeout=60000)
        await pagina.wait_for_load_state("networkidle")
        dados = await rolar_e_coletar(pagina, limite)
        return dados
    except Exception as e:
        print(f"Erro ao processar {marca}: {e}")
        traceback.print_exc()
        return []
    finally:
        await pagina.close()

async def main():
    links_existentes = carregar_progresso()
    async with async_playwright() as p:
        navegador = await p.chromium.launch(headless=True)
        for i in range(0, len(links), 5):
            lote = links[i:i+5]
            tarefas = [processar_url(navegador, m, u, l, links_existentes) for m, u, l in lote]
            resultados = await asyncio.gather(*tarefas)
            dados_totais = [item for sublista in resultados for item in sublista if sublista]
            if dados_totais:
                salvar_progresso(dados_totais)
        await navegador.close()

    print("Extração finalizada!")

    print("\nConvertendo o arquivo pickle para Excel...")
    dados_finais = carregar_progresso()

    if dados_finais:
        df = pd.DataFrame([{"Link": link} for link in dados_finais])
        df.to_excel(ARQUIVO_EXCEL, index=False)
        print(f"Arquivo Excel '{ARQUIVO_EXCEL}' gerado com sucesso!")
    else:
        print("Nenhum dado foi encontrado no arquivo pickle para conversão.")

if __name__ == "__main__":
    asyncio.run(main())

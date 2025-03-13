import pandas as pd
import os
from playwright.async_api import async_playwright
import asyncio
import sys
import pickle
import time

sys.stdout.reconfigure(encoding='utf-8')

# Arquivos de entrada e saída
ARQUIVO_PKL_LINKS = "links_chaves_na_mao_carros.pkl"
ARQUIVO_PKL_DADOS = "dados_chaves_na_mao.pkl"
ARQUIVO_EXCEL_DADOS = "dados_chaves_na_mao.xlsx"
DOMINIO_BASE = "https://www.chavesnamao.com.br/"

# Lista de links a serem extraídos 55.608 itens
links_cidades = [
    ("São José do Rio Preto", "https://www.chavesnamao.com.br/carros-usados/sp-sao-jose-do-rio-preto/", 286),
    ("Botucatu", "https://www.chavesnamao.com.br/carros-usados/sp-botucatu/", 292),
    ("Mogi das Cruzes", "https://www.chavesnamao.com.br/carros-usados/sp-mogi-das-cruzes/", 316),
    ("Limeira", "https://www.chavesnamao.com.br/carros-usados/sp-limeira/", 317),
    ("Jundiaí", "https://www.chavesnamao.com.br/carros-usados/sp-jundiai/", 363),
    ("Guarulhos", "https://www.chavesnamao.com.br/carros-usados/sp-guarulhos/", 378),
    ("Santo André", "https://www.chavesnamao.com.br/carros-usados/sp-santo-andre/", 403),
    ("São Bernardo do Campo", "https://www.chavesnamao.com.br/carros-usados/sp-sao-bernardo-do-campo/", 543),
    ("Piracicaba", "https://www.chavesnamao.com.br/carros-usados/sp-piracicaba/", 549),
    ("Santos", "https://www.chavesnamao.com.br/carros-usados/sp-santos/", 619),
    ("Osasco", "https://www.chavesnamao.com.br/carros-usados/sp-osasco/", 682),
    ("Ribeirão Preto", "https://www.chavesnamao.com.br/carros-usados/sp-ribeirao-preto/", 981),
    ("São José Dos Campos", "https://www.chavesnamao.com.br/carros-usados/sp-sao-jose-dos-campos/", 997),
    ("Sorocaba", "https://www.chavesnamao.com.br/carros-usados/sp-sorocaba/", 1009),
    ("Campinas", "https://www.chavesnamao.com.br/carros-usados/sp-campinas/", 2490),
    ("São Paulo", "https://www.chavesnamao.com.br/carros-usados/sp-sao-paulo/", 3350),
    ("Porto Alegre", "https://www.chavesnamao.com.br/carros-usados/rs-porto-alegre/", 1808),
    ("FIAT", "https://www.chavesnamao.com.br/carros/pr-curitiba/fiat/?filtro=cid:[9668],amin:2002,amax:0,or:4", 1525),
    ("GM", "https://www.chavesnamao.com.br/carros/pr-curitiba/chevrolet/?filtro=cid:[9668],amin:2002,amax:0,or:4", 1822),
    ("HYUNDAI", "https://www.chavesnamao.com.br/carros/pr-curitiba/hyundai/2002/", 602),
    ("FIAT", "https://www.chavesnamao.com.br/carros/brasil/fiat/2002/?filtro=or:2", 3000),
    ("FIAT", "https://www.chavesnamao.com.br/carros/brasil/fiat/2002/?&filtro=or:4", 3000),
    ("FIAT", "https://www.chavesnamao.com.br/carros/brasil/fiat/2002/?&filtro=or:2", 3000),
    ("Volkswagen", "https://www.chavesnamao.com.br/carros/brasil/volkswagen/2002/?filtro=or:2", 3000),
    ("Volkswagen", "https://www.chavesnamao.com.br/carros/brasil/volkswagen/2002/?filtro=or:4", 3000),
    ("Chevrolet", "https://www.chavesnamao.com.br/carros/brasil/chevrolet/2002/?filtro=or:2", 3000),
    ("Chevrolet", "https://www.chavesnamao.com.br/carros/brasil/chevrolet/2002/?filtro=or:2", 3000),
    ("HYUNDAI", "https://www.chavesnamao.com.br/carros/brasil/hyundai/2002/?filtro=or:2", 3000),
    ("Citroën", "https://www.chavesnamao.com.br/carros/brasil/citroen/2002/?filtro=or:2", 1480),
    ("FORD", "https://www.chavesnamao.com.br/carros/brasil/ford/2002/?filtro=or:2", 3000),
    ("HONDA", "https://www.chavesnamao.com.br/carros/brasil/honda/2002/?filtro=or:2", 2213),
    ("JEEP", "https://www.chavesnamao.com.br/carros/brasil/honda/2002/?filtro=or:2", 2900),
    ("TOYOTA", "https://www.chavesnamao.com.br/carros/brasil/toyota/2002/?filtro=or:2", 2000),
    ("AUDI", "https://www.chavesnamao.com.br/carros/brasil/audi/2002/?filtro=or:2", 800),
    ("BMW", "https://www.chavesnamao.com.br/carros/brasil/bmw/2002/?filtro=or:2", 1000),
    ("CHERY", "https://www.chavesnamao.com.br/carros/brasil/caoa-chery/2002/?filtro=or:2", 558),
    ("KIA", "https://www.chavesnamao.com.br/carros/brasil/kia/2002/?filtro=or:2", 714),
    ("LAND ROVER", "https://www.chavesnamao.com.br/carros/brasil/land-rover/2002/?filtro=or:2", 608),
    ("MERCEDES", "https://www.chavesnamao.com.br/carros/brasil/mercedes-benz/2002/?filtro=or:2", 900),
    ("PORSCHE", "https://www.chavesnamao.com.br/carros/brasil/porsche/2002/?filtro=or:2", 300),
    ("Mitsubishi", "https://www.chavesnamao.com.br/carros/brasil/mitsubishi/2002/?filtro=or:2", 1130),
    ("Peugeot", "https://www.chavesnamao.com.br/carros/brasil/peugeot/2002/?filtro=or:2", 1500),
    ("Renault", "https://www.chavesnamao.com.br/carros/brasil/renault/2002/?filtro=or:2", 3600),
    ("RAM", "https://www.chavesnamao.com.br/carros/brasil/ram/2002/?filtro=or:2", 242),
    ("VOLVO", "https://www.chavesnamao.com.br/carros/brasil/volvo/2002/?filtro=or:2", 338),
    ("SUZUKI", "https://www.chavesnamao.com.br/carros/brasil/suzuki/2002/?filtro=or:2", 170),
    ("BYD", "https://www.chavesnamao.com.br/carros/brasil/byd/2002/?filtro=or:2", 91),
    ("MINI", "https://www.chavesnamao.com.br/carros/brasil/mini/2002/?filtro=or:2", 213),
    ("TROLLER", "https://www.chavesnamao.com.br/carros/brasil/troller/2002/", 104),
    ("Jaguar", "https://www.chavesnamao.com.br/carros/brasil/jaguar/2002/", 95),
    ("DODGE", "https://www.chavesnamao.com.br/carros/brasil/dodge/2002/?filtro=or:2", 85),
    ("JAC", "https://www.chavesnamao.com.br/carros/brasil/jac/2002/", 64),
    ("GWM", "https://www.chavesnamao.com.br/carros/brasil/gwm/2002/?filtro=or:2", 46),
    ("SMART", "https://www.chavesnamao.com.br/carros/brasil/smart/2002/", 38),
    ("Chrysler", "https://www.chavesnamao.com.br/carros/brasil/chrysler/2002/", 35),
    ("Lexus", "https://www.chavesnamao.com.br/carros/brasil/lexus/2002/", 25),
    ("LIFAN", "https://www.chavesnamao.com.br/carros/brasil/lifan/2002/", 22),
    ("Ssangyong", "https://www.chavesnamao.com.br/carros/brasil/ssangyong/2002/", 13),
    ("Iveco", "https://www.chavesnamao.com.br/carros/brasil/iveco/2002/", 12),
    ("EEFA", "https://www.chavesnamao.com.br/carros/brasil/effa/2002/", 11),
    ("TESLA", "https://www.chavesnamao.com.br/carros/brasil/tesla/2002/", 8),
    ("Shineray", "https://www.chavesnamao.com.br/carros/brasil/shineray/2002/", 6),
    ("FERRARI", "https://www.chavesnamao.com.br/carros/brasil/ferrari/2002/", 5),
    ("Infiniti", "https://www.chavesnamao.com.br/carros/brasil/infiniti/2002/", 4),
    ("ASIA", "https://www.chavesnamao.com.br/carros/brasil/asia/2002/", 2),
    ("Cadillac", "https://www.chavesnamao.com.br/carros/brasil/cadillac/2002/", 2),
    ("Mclaren", "https://www.chavesnamao.com.br/carros/brasil/mclaren/2002/", 2),
    ("Agrale", "https://www.chavesnamao.com.br/carros/brasil/agrale/2002/", 1),
    ("Bentley", "https://www.chavesnamao.com.br/carros/brasil/bentley/2002/", 1),
    ("BRM", "https://www.chavesnamao.com.br/carros/brasil/brm/2002/", 1),
    ("Outros", "https://www.chavesnamao.com.br/carros/pr-curitiba/fiat-palio/?filtro=cid:[9668],amin:2002,amax:0,or:4", 50),
]

def salvar_progresso(dados):
    if os.path.exists(ARQUIVO_PKL_LINKS):
        with open(ARQUIVO_PKL_LINKS, "rb") as f:
            dados_existentes = pickle.load(f)
    else:
        dados_existentes = []

    dados_existentes.extend(dados)

    with open(ARQUIVO_PKL_LINKS, "wb") as f:
        pickle.dump(dados_existentes, f)
    print(f"Progresso salvo em '{ARQUIVO_PKL_LINKS}'")

# Função para carregar links salvos anteriormente
def carregar_links():
    if os.path.exists(ARQUIVO_PKL_LINKS):
        with open(ARQUIVO_PKL_LINKS, "rb") as f:
            dados = pickle.load(f)
        links = []
        for item in dados:
            if isinstance(item, dict) and 'Link' in item:
                links.append(item['Link'])
            elif isinstance(item, str):
                links.append(item)
        print(f"🔹 {len(links)} links carregados do arquivo existente.")
        return links
    return []

# Função assíncrona para rolar a página e coletar links
async def rolar_ate_todos_itens_carregarem(pagina, limite_itens):
    tentativas_sem_novos_itens = 0
    ids_itens_carregados = set()
    altura_anterior = 0

    while tentativas_sem_novos_itens < 5:
        if len(ids_itens_carregados) >= limite_itens:
            print(f"Limite de {limite_itens} itens atingido. Parando a busca.")
            break

        try:
            await pagina.wait_for_selector('//a[@href]', timeout=10000)
            links = pagina.locator('//a[@href]')
            total_links = await links.count()
        except Exception as e:
            print(f"Erro ao encontrar links: {e}. Tentando novamente...")
            total_links = 0

        novos_itens = 0
        for i in range(total_links):
            if len(ids_itens_carregados) >= limite_itens:
                break
            try:
                id_item = await links.nth(i).get_attribute('href')
                if id_item:
                    id_item = id_item if id_item.startswith("http") else f"{DOMINIO_BASE}{id_item}"
                    id_item = id_item.replace("//", "/").replace("https:/", "https://")
                    if "/carro/" in id_item and "/id-" in id_item and id_item not in ids_itens_carregados:
                        ids_itens_carregados.add(id_item)
                        novos_itens += 1
            except:
                continue

        print(f"Total de links coletados até agora: {len(ids_itens_carregados)}")

        altura_atual = await pagina.evaluate("document.body.scrollHeight")
        if altura_atual == altura_anterior and novos_itens == 0:
            tentativas_sem_novos_itens += 1
        else:
            tentativas_sem_novos_itens = 0
            altura_anterior = altura_atual

        await pagina.evaluate("window.scrollBy(0, document.body.scrollHeight);")
        await asyncio.sleep(3)

    print(f"Fim da rolagem para esta página. Total de itens carregados: {len(ids_itens_carregados)}")
    return list(ids_itens_carregados)

# Função assíncrona para extrair links
async def extracao_links(navegador, cidade, url, limite):
    pagina = await navegador.new_page()
    print(f"\nAbrindo página para {cidade} ({url})...")
    try:
        await pagina.goto(url, timeout=30000)
        await pagina.wait_for_load_state("networkidle")
        links_coletados = await rolar_ate_todos_itens_carregarem(pagina, limite)
        await pagina.close()
        return [{"Link": link} for link in links_coletados]
    except Exception as e:
        print(f"Erro ao processar {cidade}: {e}")
        await pagina.close()
        return []

# Função assíncrona para extrair dados de cada link
async def extracaoDados(navegador, link):
    pagina = await navegador.new_page()
    print(f"Acessando {link}...")

    try:
        await pagina.goto(link, timeout=20000)
        await pagina.wait_for_load_state('domcontentloaded', timeout=20000)

        async def extrair(xpath, index=0):
            elementos = pagina.locator(xpath)
            count = await elementos.count()
            if count > index:
                return await elementos.nth(index).inner_text()
            return "N/A"

        dados = {
            "Modelo": await extrair('//article//section[2]//div//div[1]//div//span//p//b', index=0),
            "Preço": await extrair('//article//section[2]//div//div[1]//div//span//p//b', index=1),
            "Versão": await extrair('//article//section[2]//div//div[1]//div//span//p//small'),
            "Subsegmento": await extrair('//article/article/section[2]/div/div[1]/ul/li[6]/p/b'),
            "Cor": await extrair('//article/article/section[2]/div/div[1]/ul/li[7]/p/b'),
            "KM": await extrair('//article//section[2]//div//div[1]//ul//li[3]//p//b'),
            "Transmissão": await extrair('//article//section[2]//div//div[1]//ul//li[4]//p//b'),
            "Fipe": await extrair('//*[@id="version-price-fipe"]/tr[1]'),
            "Ano do Modelo": await extrair('//article//section[2]//div//div[1]//ul//li[2]//p//b'),
            "Combustível": await extrair('//article//section[2]//div//div[1]//ul//li[5]//p//b'),
            "Localização": await extrair('//article//section[2]//div//div[1]//ul//li[1]//p//b'),
            "Anunciante": await extrair('//*[@id="aside-init"]/div[2]/span'),
            "Cidade": "Desconhecido",
            "Link": link
        }
        if " - " in dados["Localização"]:
            dados["Cidade"] = dados["Localização"].split(" - ")[0]

        return dados

    except Exception as e:
        print(f"Erro em {link}: {e}")
        return None
    
    finally:
        await pagina.close()

# Função assíncrona para coletar todos os links
async def coletar_todos_links():
    async with async_playwright() as p:
        navegador = await p.chromium.launch(headless=False)  # headless=False para ver o processo
        todos_links = []

        for cidade, url, limite in links_cidades:
            dados = await extracao_links(navegador, cidade, url, limite)
            if dados:
                todos_links.extend(dados)
                salvar_progresso(dados)

        await navegador.close()
    return [item['Link'] for item in todos_links]

# Função assíncrona para processar links e extrair dados
async def processar_links(links, max_concurrent=4):
    dados_coletados = []
    async with async_playwright() as p:
        navegador = await p.chromium.launch(headless=True)
        
        for i in range(0, len(links), max_concurrent):
            batch = links[i:i + max_concurrent]
            tasks = [extracaoDados(navegador, link) for link in batch]
            resultados = await asyncio.gather(*tasks, return_exceptions=True)
            for resultado in resultados:
                if resultado and not isinstance(resultado, Exception):
                    dados_coletados.append(resultado)
        
        await navegador.close()
    
    return dados_coletados

async def main_async():
    start_time = time.time()
    
    # Passo 1: Coletar links se o arquivo não existir ou estiver incompleto
    links_existentes = carregar_links()
    if not links_existentes:
        print("Coletando novos links...")
        links = await coletar_todos_links()
    else:
        print("Usando links existentes...")
        links = links_existentes

    if not links:
        print("Nenhum link para processar.")
        return

    # Passo 2: Processar os links e extrair dados
    print(f"Processando {len(links)} links...")
    dados_coletados = await processar_links(links, max_concurrent=4)

    # Passo 3: Salvar os dados coletados
    if dados_coletados:
        df = pd.DataFrame(dados_coletados)
        df.to_pickle(ARQUIVO_PKL_DADOS)
        print(f"Dados exportados para '{ARQUIVO_PKL_DADOS}'.")

        salvar_excel = input("Deseja salvar em Excel também? (sim/não): ").strip().lower()
        if salvar_excel == "sim":
            df.to_excel(ARQUIVO_EXCEL_DADOS, index=False)
            print(f"Dados exportados para '{ARQUIVO_EXCEL_DADOS}'.")
    else:
        print("Nenhum dado foi extraído.")

    print(f"Tempo total: {time.time() - start_time:.2f} segundos")

def main():
    asyncio.run(main_async())

if __name__ == "__main__":
    main()

from playwright.async_api import async_playwright
import asyncio
import os
import pickle
import pandas as pd  

links = [
    ("Renault", "https://www.chavesnamao.com.br/carros/brasil/renault-sandero/2002/", 3000),
    ("FORD", "https://www.chavesnamao.com.br/carros/brasil/ford/2002/?filtro=or:2", 3000),
    ("São Paulo", "https://www.chavesnamao.com.br/carros-usados/sp-sao-paulo/", 3000),
    ("PCD", "https://www.chavesnamao.com.br/carros-para-pcd/brasil/?&filtro=amin:2002,amax:0,ne:[4],or:0", 3000),
    ("Carro", "https://www.chavesnamao.com.br/carros-usados/brasil/?&filtro=amin:2002,amax:0,or:4", 2500),
    ("JEEP", "https://www.chavesnamao.com.br/carros/brasil/honda/2002/?filtro=or:2", 2900),
    ("Chevrolet", "https://www.chavesnamao.com.br/carros/brasil/chevrolet-onix/2002/", 2749),
    ("Campinas", "https://www.chavesnamao.com.br/carros-usados/sp-campinas/", 2490),
    ("HONDA", "https://www.chavesnamao.com.br/carros/brasil/honda/2002/?filtro=or:2", 2213),
    ("TOYOTA", "https://www.chavesnamao.com.br/carros/brasil/toyota/2002/?filtro=or:2", 2000),
    ("Rj", "https://www.chavesnamao.com.br/carros-usados/rj-rio-de-janeiro/?filtro=amin:2002,amax:0,or:1", 2000),
    ("São Paulo", "https://www.chavesnamao.com.br/carros-usados/sp-sao-paulo/?&filtro=amin:2002,amax:0,or:1", 2000),
    ("São Paulo", "https://www.chavesnamao.com.br/carros-usados/sp-sao-paulo/?&filtro=amin:2002,amax:0,or:4", 2000),
    ("Porto Alegre", "https://www.chavesnamao.com.br/carros-usados/rs-porto-alegre/", 1808),
    ("Curitiba", "https://www.chavesnamao.com.br/carros-usados/pr-curitiba/?&filtro=amin:2002,amax:0,or:1", 1000),
    ("Curitiba", "https://www.chavesnamao.com.br/carros-usados/pr-curitiba/?&filtro=amin:2002,amax:0,or:2", 1000),
    ("Curitiba", "https://www.chavesnamao.com.br/carros-usados/pr-curitiba/?&filtro=amin:2002,amax:0,or:3", 3000),
    ("Curitiba", "https://www.chavesnamao.com.br/carros-usados/pr-curitiba/?&filtro=amin:2002,amax:0,or:4", 3000),
    ("FIAT", "https://www.chavesnamao.com.br/carros/brasil/fiat-toro/2002/", 849),
    ("FIAT", "https://www.chavesnamao.com.br/carros/brasil/fiat-uno/2002/", 534),
    ("FIAT", "https://www.chavesnamao.com.br/carros/brasil/fiat-strada/2002/", 859),
    ("FIAT", "https://www.chavesnamao.com.br/carros/brasil/fiat-argo/2002/", 686),
    ("FIAT", "https://www.chavesnamao.com.br/carros/brasil/fiat-cronos/2002/", 394),
    ("FIAT", "https://www.chavesnamao.com.br/carros/brasil/fiat-fiorino/2002/", 287),
    ("FIAT", "https://www.chavesnamao.com.br/carros/brasil/fiat-pulse/2002/", 234),
    ("FIAT", "https://www.chavesnamao.com.br/carros/brasil/fiat-fastback/2002/", 192),
    ("FIAT", "https://www.chavesnamao.com.br/carros/brasil/fiat-idea/2002/", 145),
    ("FIAT", "https://www.chavesnamao.com.br/carros/brasil/fiat-siena/2002/", 191),
    ("FIAT", "https://www.chavesnamao.com.br/carros/brasil/fiat-grand-siena/2002/", 120),
    ("FIAT", "https://www.chavesnamao.com.br/carros/brasil/fiat-punto/2002/", 114),
    ("FIAT", "https://www.chavesnamao.com.br/carros/brasil/fiat-500/2002/", 112),
    ("FIAT", "https://www.chavesnamao.com.br/carros/brasil/fiat-freemont/2002/", 63),
    ("FIAT", "https://www.chavesnamao.com.br/carros/brasil/fiat-palio-weekend/2002/", 51),
    ("FIAT", "https://www.chavesnamao.com.br/carros/brasil/fiat-doblo/2002/", 58),
    ("Volkswagen", "https://www.chavesnamao.com.br/carros/brasil/volkswagen-t-cross/2002/", 1056),
    ("Volkswagen", "https://www.chavesnamao.com.br/carros/brasil/volkswagen-polo/2002/", 984),
    ("Volkswagen", "https://www.chavesnamao.com.br/carros/brasil/volkswagen-gol/2002/", 1016),
    ("Volkswagen", "https://www.chavesnamao.com.br/carros/brasil/volkswagen-fox/2002/", 760),
    ("Volkswagen", "https://www.chavesnamao.com.br/carros/brasil/volkswagen-saveiro/2002/", 767),
    ("Volkswagen", "https://www.chavesnamao.com.br/carros/brasil/volkswagen-nivus/2002/", 677),
    ("Volkswagen", "https://www.chavesnamao.com.br/carros/brasil/volkswagen-voyage/2002/", 513),
    ("Volkswagen", "https://www.chavesnamao.com.br/carros/brasil/volkswagen-amarok/2002/", 410),
    ("Volkswagen", "https://www.chavesnamao.com.br/carros/brasil/volkswagen-up/2002/", 433),
    ("Volkswagen", "https://www.chavesnamao.com.br/carros/brasil/volkswagen-virtus/2002/", 373),
    ("Volkswagen", "https://www.chavesnamao.com.br/carros/brasil/volkswagen-golf/2002/", 251),
    ("Volkswagen", "https://www.chavesnamao.com.br/carros/brasil/volkswagen-tiguan/2002/", 264),
    ("Volkswagen", "https://www.chavesnamao.com.br/carros/brasil/volkswagen-taos/2002/", 143),
    ("Volkswagen", "https://www.chavesnamao.com.br/carros/brasil/volkswagen-spacefox/2002/", 112),
    ("Volkswagen", "https://www.chavesnamao.com.br/carros/brasil/volkswagen-kombi/2002/", 47),
    ("Volkswagen", "https://www.chavesnamao.com.br/carros/brasil/volkswagen-parati/2002/", 37),
    ("Chevrolet", "https://www.chavesnamao.com.br/carros/brasil/chevrolet-tracker/2002/", 1593),
    ("Chevrolet", "https://www.chavesnamao.com.br/carros/brasil/chevrolet-cruze/2002/", 604),
    ("Chevrolet", "https://www.chavesnamao.com.br/carros/brasil/chevrolet-s10/2002/", 561),
    ("Chevrolet", "https://www.chavesnamao.com.br/carros/brasil/chevrolet-montana/2002/", 322),
    ("Chevrolet", "https://www.chavesnamao.com.br/carros/brasil/chevrolet-prisma/2002/", 418),
    ("Chevrolet", "https://www.chavesnamao.com.br/carros/brasil/chevrolet-spin/2002/", 485),
    ("Chevrolet", "https://www.chavesnamao.com.br/carros/brasil/chevrolet-classic/2002/", 143),
    ("Chevrolet", "https://www.chavesnamao.com.br/carros/brasil/chevrolet-corsa/2002/", 126),
    ("Chevrolet", "https://www.chavesnamao.com.br/carros/brasil/chevrolet-cobalt/2002/", 135),
    ("Chevrolet", "https://www.chavesnamao.com.br/carros/brasil/chevrolet-celta/2002/", 224),
    ("Chevrolet", "https://www.chavesnamao.com.br/carros/brasil/chevrolet-agile/2002/", 99),
    ("Chevrolet", "https://www.chavesnamao.com.br/carros/brasil/chevrolet-vectra/2002/", 85),
    ("HYUNDAI", "https://www.chavesnamao.com.br/carros/brasil/hyundai-hb20/2002/", 1297),
    ("HYUNDAI", "https://www.chavesnamao.com.br/carros/brasil/hyundai-hb20s/2002/", 617),
    ("HYUNDAI", "https://www.chavesnamao.com.br/carros/brasil/hyundai-creta/2002/", 605),
    ("HYUNDAI", "https://www.chavesnamao.com.br/carros/brasil/hyundai-tucson/2002/", 349),
    ("HYUNDAI", "https://www.chavesnamao.com.br/carros/brasil/hyundai-ix35/2002/", 265),
    ("Franca", "https://www.chavesnamao.com.br/carros-usados/sp-franca/?filtro=amin:2002,amax:0,or:4", 269),
    ("HYUNDAI", "https://www.chavesnamao.com.br/carros/brasil/hyundai-i30/2002/", 98),
    ("HYUNDAI", "https://www.chavesnamao.com.br/carros/brasil/hyundai-santa-fe/2002/", 89),
    ("HYUNDAI", "https://www.chavesnamao.com.br/carros/brasil/hyundai-hr/2002/", 51),
    ("Citroën", "https://www.chavesnamao.com.br/carros/brasil/citroen/2002/?filtro=or:2", 1480),
    ("Peugeot", "https://www.chavesnamao.com.br/carros/brasil/peugeot/2002/?filtro=or:2", 1500),
    ("Mitsubishi", "https://www.chavesnamao.com.br/carros/brasil/mitsubishi/2002/?filtro=or:2", 1130),
    ("Sorocaba", "https://www.chavesnamao.com.br/carros-usados/sp-sorocaba/", 1009),
    ("Ribeirão Preto", "https://www.chavesnamao.com.br/carros-usados/sp-ribeirao-preto/", 981),
    ("São José Dos Campos", "https://www.chavesnamao.com.br/carros-usados/sp-sao-jose-dos-campos/", 997),
    ("São José do Rio Preto", "https://www.chavesnamao.com.br/carros-usados/sp-sao-jose-do-rio-preto/", 286),
    ("Botucatu", "https://www.chavesnamao.com.br/carros-usados/sp-botucatu/", 292),
    ("Mogi das Cruzes", "https://www.chavesnamao.com.br/carros-usados/sp-mogi-das-cruzes/", 316),
    ("Limeira", "https://www.chavesnamao.com.br/carros-usados/sp-limeira/", 317),
    ("Jundiaí", "https://www.chavesnamao.com.br/carros-usados/sp-jundiai/", 363),
    ("Guarulhos", "https://www.chavesnamao.com.br/carros-usados/sp-guarulhos/", 378),
    ("Carro", "https://www.chavesnamao.com.br/carros-7-lugares/brasil/?&filtro=amin:2002,amax:0,ne:[2],or:0", 300),
    ("Santo André", "https://www.chavesnamao.com.br/carros-usados/sp-santo-andre/", 403),
    ("São Bernardo do Campo", "https://www.chavesnamao.com.br/carros-usados/sp-sao-bernardo-do-campo/", 543),
    ("Piracicaba", "https://www.chavesnamao.com.br/carros-usados/sp-piracicaba/", 549),
    ("Santos", "https://www.chavesnamao.com.br/carros-usados/sp-santos/", 619),
    ("Osasco", "https://www.chavesnamao.com.br/carros-usados/sp-osasco/", 682),
    ("HYUNDAI", "https://www.chavesnamao.com.br/carros/pr-curitiba/hyundai/2002/", 602),
    ("AUDI", "https://www.chavesnamao.com.br/carros/brasil/audi/2002/?filtro=or:2", 800),
    ("BMW", "https://www.chavesnamao.com.br/carros/brasil/bmw/2002/?filtro=or:2", 1000),
    ("CHERY", "https://www.chavesnamao.com.br/carros/brasil/caoa-chery/2002/?filtro=or:2", 558),
    ("KIA", "https://www.chavesnamao.com.br/carros/brasil/kia/2002/?filtro=or:2", 714),
    ("LAND ROVER", "https://www.chavesnamao.com.br/carros/brasil/land-rover/2002/?filtro=or:2", 608),
    ("MERCEDES", "https://www.chavesnamao.com.br/carros/brasil/mercedes-benz/2002/?filtro=or:2", 900),
    ("PORSCHE", "https://www.chavesnamao.com.br/carros/brasil/porsche/2002/?filtro=or:2", 300),
    ("RAM", "https://www.chavesnamao.com.br/carros/brasil/ram/2002/?filtro=or:2", 242),
    ("VOLVO", "https://www.chavesnamao.com.br/carros/brasil/volvo/2002/?filtro=or:2", 338),
    ("SUZUKI", "https://www.chavesnamao.com.br/carros/brasil/suzuki/2002/?filtro=or:2", 170),
    ("BYD", "https://www.chavesnamao.com.br/carros/brasil/byd/2002/?filtro=or:2", 91),
    ("MINI", "https://www.chavesnamao.com.br/carros/brasil/mini/2002/?filtro=or:2", 213),
    ("TROLLER", "https://www.chavesnamao.com.br/carros/brasil/troller/2002/", 104),
    ("Jaguar", "https://www.chavesnamao.com.br/carros/brasil/jaguar/2002/", 95),
    ("DODGE", "https://www.chavesnamao.com.br/carros/brasil/dodge/2002/?filtro=or:2", 85),
    ("Antigos", "https://www.chavesnamao.com.br/carros-antigos/brasil/?filtro=ne:[3],amin:2002,amax:0,or:2", 78),
    ("JAC", "https://www.chavesnamao.com.br/carros/brasil/jac/2002/", 64),
    ("GWM", "https://www.chavesnamao.com.br/carros/brasil/gwm/2002/?filtro=or:2", 46),
    ("SMART", "https://www.chavesnamao.com.br/carros/brasil/smart/2002/", 38),
    ("Chrysler", "https://www.chavesnamao.com.br/carros/brasil/chrysler/2002/", 35),
    ("Eletricos", "https://www.chavesnamao.com.br/carros-eletricos/brasil/?&filtro=amin:2002,amax:0,ne:[6],or:0", 38),
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
]

#Onde sera salvo os links coletados
ARQUIVO_PICKLE = "links_chaves_na_mao_carros.pkl"
ARQUIVO_EXCEL = "links_chaves_na_mao_carros.xlsx"  
DOMINIO_BASE = "https://www.chavesnamao.com.br/"

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

#Função para rolar a página e coletar os links 
async def rolar_e_coletar(pagina, limite_itens):
    ids_itens_carregados = set()
    tentativas_sem_novos_itens = 0
    altura_anterior = 0

    while tentativas_sem_novos_itens < 5 and len(ids_itens_carregados) < limite_itens:
        try:
            await pagina.wait_for_selector('//a[@href]', timeout=10000)
            links = await pagina.query_selector_all('//a[@href]')
            novos_itens = 0

            for link in links:
                if len(ids_itens_carregados) >= limite_itens:
                    break
                href = await link.get_attribute('href')
                if href:
                    href = href if href.startswith("http") else f"{DOMINIO_BASE}{href}"
                    href = href.replace("//", "/").replace("https:/", "https://")
                    if "/carro/" in href and "/id-" in href and href not in ids_itens_carregados:
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
            print(f"Erro ao encontrar links: {e}. Tentando novamente...")
            tentativas_sem_novos_itens += 1
            await asyncio.sleep(3)

    print(f"Fim da rolagem para esta página. Total de itens carregados: {len(ids_itens_carregados)}")
    return [{"Link": link} for link in ids_itens_carregados]

#Verifica se não é algum anuncio e sim um link 
async def processar_url(navegador, cidade, url, limite, links_existentes):
    if url in links_existentes:
        print(f"{cidade} já processado. Pulando...")
        return []

    print(f"Processando {cidade} ({url})...")
    pagina = await navegador.new_page()
    try:
        await pagina.route("**/*.{png,jpg,jpeg,gif,webp}", lambda route: route.abort())
        await pagina.goto(url, timeout=60000)
        await pagina.wait_for_load_state("domcontentloaded")
        dados = await rolar_e_coletar(pagina, limite)
        return dados
    except Exception as e:
        print(f"Erro ao processar {cidade}: {e}")
        return []
    finally:
        await pagina.close()

def salvar_em_excel(dados_totais):
    if not dados_totais:
        print("Nenhum dado para salvar em Excel.")
        return

    df = pd.DataFrame(dados_totais)
    df = df.drop_duplicates(subset=["Link"])
    df.to_excel(ARQUIVO_EXCEL, index=False)
    print(f"Dados salvos em '{ARQUIVO_EXCEL}' com {len(df)} registros.")

async def main():
    links_existentes = carregar_progresso()
    dados_totais = [] 
    
    async with async_playwright() as p:
        navegador = await p.chromium.launch()
        for i in range(0, len(links), 10):
            lote = links[i:i+10]
            tarefas = [processar_url(navegador, c, u, l, links_existentes) for c, u, l in lote]
            resultados = await asyncio.gather(*tarefas)
            novos_dados = [item for sublista in resultados for item in sublista if sublista]
            if novos_dados:
                dados_totais.extend(novos_dados)
                salvar_progresso(novos_dados)
        await navegador.close()

    salvar_em_excel(dados_totais)

if __name__ == "__main__":
    asyncio.run(main())
    print("Extração finalizada!")


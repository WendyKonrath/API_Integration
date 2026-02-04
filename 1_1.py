import os
import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/"

HEADERS = {"User-Agent": "Mozilla/5.0"}

def listar_links(url):
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    links = []
    for a in soup.find_all("a"):
        href = a.get("href")
        if href and not href.startswith("?"):
            links.append(urljoin(url, href))
    return links

def listar_anos(links):
    anos = []
    for link in links:
        m = re.search(r"/(\d{4})/?$", link)
        if m:
            anos.append(m.group(1))

    anos.sort(reverse=True)
    return anos

def trimestres_encontrados():
    # 1. Obtém os links da pasta raiz para descobrir os anos
    links_raiz = listar_links(BASE_URL)
    anos = listar_anos(links_raiz)

    trimestres_coletados = []

    # 2. Itera sobre os anos (do mais recente para o mais antigo)
    for ano in anos:
        url_ano = urljoin(BASE_URL, f"{ano}/")
        links_ano = listar_links(url_ano)

        # Filtra links que correspondem ao padrão de trimestre (ex: 1T2025)
        # Ordena reverso para pegar 4T antes de 3T, etc.
        trimes_ano = [link for link in links_ano if re.search(r"\d+T\d{4}", link, re.IGNORECASE)]
        trimes_ano.sort(reverse=True)

        trimestres_coletados.extend(trimes_ano)

        # Se já temos 3 ou mais, paramos de procurar em anos anteriores
        if len(trimestres_coletados) >= 3:
            break

    # 3. Exibe e retorna apenas os 3 mais recentes
    resultado = trimestres_coletados[:3]
    return resultado

def baixar_trimestres(url, pasta="trimestres_baixados"):
    os.makedirs(pasta, exist_ok=True)
    nome = url.split("/")[-1]
    caminho = os.path.join(pasta, nome)

    with requests.get(url, headers=HEADERS, stream=True) as r:
        r.raise_for_status()
        with open(caminho, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    return caminho

def main():
    links = trimestres_encontrados()
    for link in links:
        print(f"Baixando: {link}")
        baixar_trimestres(link)

if __name__ == "__main__":
    main()

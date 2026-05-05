import fitz
from PIL import Image
import io
from pathlib import Path

entrada = Path(r'C:\Users\wconceicao\OneDrive - Grupo A Educação SA\Área de Trabalho\NOMES.pdf')
saida   = Path(r'C:\Users\wconceicao\OneDrive - Grupo A Educação SA\Área de Trabalho\NOMES_SEM_FUNDO.pdf')

doc = fitz.open(entrada)
doc_saida = fitz.open()

for pagina in doc:
    pix = pagina.get_pixmap(dpi=200)
    img = Image.open(io.BytesIO(pix.tobytes("png"))).convert("RGBA")

    dados = img.getdata()
    nova = []
    for pixel in dados:
        r, g, b, a = pixel
        if r > 230 and g > 230 and b > 230:  # branco/quase branco → transparente
            nova.append((255, 255, 255, 0))
        else:
            nova.append(pixel)
    img.putdata(nova)

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)

    rect = pagina.rect
    nova_pag = doc_saida.new_page(width=rect.width, height=rect.height)
    nova_pag.insert_image(rect, stream=buf.read())

doc_saida.save(saida)
print(f'Salvo em: {saida}')
# licitaberto

Licitacións de concelhos da Plataforma de Contratación do Sector Público: [https://contrataciondelestado.es/](https://contrataciondelestado.es/)

## Que

Frontend de licitacións dun concelho/s dado/s para revisar visual e rapidamente as contratacións e licitacións dadas de alta na Plataforma de Contratación do Sector Público.

## Como

O proxecto ten 2 componhentes:

- Scrapper: réplica e afinado de [https://github.com/alexandregz/plataforma_contratacion_estado_scrapper](https://github.com/alexandregz/plataforma_contratacion_estado_scrapper)
- Front-end: acceso aos datos parseados polo `scrapper` de xeito cómodo.

## Requerimentos

- [`bun`](https://bun.com/) (ou `node`)
- [`puppeteer`](https://pptr.dev/)
- `sqlite`
- `PHP`

## Instalación

Pódese empregar fisicamente ou empregar `Docker`(recomendado).

## Uso

```bash
alex@vosjod:~/Development/licitaberto $ go run . --db ../plataforma_contratacion_estado_scrapper/ames.db --mode web --addr 127.0.0.1:8080
2025/09/12 00:42:50 Web UI en http://127.0.0.1:8080
2025/09/12 00:42:50 PDFs en ../plataforma_contratacion_estado_scrapper/PDF/ames
```

## Uso TUI

ToDo, sen uso efectivo actualmente!.

```bash
alex@vosjod:~/Development/licitaberto $ go run . --db ../plataforma_contratacion_estado_scrapper/san_cibrao_das_vinhas.db --mode tui
``` 


## Docker

ToDo


## Screenshots

![Listado de taboas](imaxes/001.png)

![Táboa](imaxes/002.png)

![Gráfica desplegada](imaxes/003.png)

![Outra tábaos](imaxes/004.png)

![Exemplo de táboa de ficheiros](imaxes/005.png)

![sumario con gráficas](imaxes/006.png)
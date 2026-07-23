# Slides da apresentação (TP03)

Geram o arquivo `../TP03_Apresentacao.pptx` mantendo a mesma identidade visual do
TP2 (navy `#1E2761` + laranja `#FF6F1E`, fontes Cambria/Calibri, cards arredondados
e ícones limpos).

## Como regenerar

```bash
cd slides
npm install            # pptxgenjs + react-icons + sharp
node gen.js            # escreve ../TP03_Apresentacao.pptx
```

- `gen.js` — monta os 8 slides (capa, continuidade TP2→TP3, problema, arquitetura,
  protocolo primário-backup, tolerância a falhas, execução Docker/Kubernetes/AWS,
  conclusão).
- `icons.js` — renderiza ícones do `react-icons` como PNG na cor certa (mesmo estilo
  dos ícones do TP2).

`node_modules/` não é versionado — rode `npm install` para recriá-lo.

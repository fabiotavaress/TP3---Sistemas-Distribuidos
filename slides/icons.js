// Renderiza icones do react-icons como PNG base64 (na cor desejada),
// para inserir no pptx com o mesmo visual limpo dos icones do TP2.
const React = require('react');
const ReactDOMServer = require('react-dom/server');
const sharp = require('sharp');
const fa = require('react-icons/fa');
const fa6 = require('react-icons/fa6');
const si = require('react-icons/si');

const REG = { ...fa, ...fa6, ...si };
const cache = {};

async function icon(name, hex, px = 256) {
  const key = `${name}_${hex}_${px}`;
  if (cache[key]) return cache[key];
  const Comp = REG[name];
  if (!Comp) throw new Error(`Icone nao encontrado: ${name}`);
  let svg = ReactDOMServer.renderToStaticMarkup(
    React.createElement(Comp, { size: px, color: `#${hex}` })
  );
  // Garante que o preenchimento use a cor certa mesmo sem contexto CSS
  svg = svg.replace(/currentColor/g, `#${hex}`);
  const buf = await sharp(Buffer.from(svg))
    .resize(px, px, { fit: 'contain', background: { r: 0, g: 0, b: 0, alpha: 0 } })
    .png()
    .toBuffer();
  const data = 'image/png;base64,' + buf.toString('base64');
  cache[key] = data;
  return data;
}

module.exports = { icon };

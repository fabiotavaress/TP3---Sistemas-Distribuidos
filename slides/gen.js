// ============================================================================
// Gerador dos slides do TP3 — mesma identidade visual do TP2
// (navy #1E2761 + laranja #FF6F1E, Cambria/Calibri, cards arredondados).
// ============================================================================
const pptxgen = require('pptxgenjs');
const { icon } = require('./icons');

// ---- Paleta (extraida do proprio TP2) ----
const NAVY = '1E2761';
const NAVY_DEEP = '212B45';
const NAVY_CARD = '222C4E';
const ORANGE = 'FF6F1E';
const WHITE = 'FFFFFF';
const CARD_LIGHT = 'F4F6FC';
const MUTED = '6B7590';
const ICE = 'CADCFC';
const PEACH = 'FFF1E8';

const TITLE_FONT = 'Cambria';
const BODY = 'Calibri';

const pres = new pptxgen();
pres.defineLayout({ name: 'T', width: 10, height: 5.625 });
pres.layout = 'T';
pres.author = 'Fabio Tavares';
pres.title = 'TP03 - Sistemas Distribuidos';

// ---------- Helpers de layout ----------

// Faixa "eyebrow" laranja com espacamento de letras (igual TP2)
function eyebrow(slide, text) {
  slide.addText(text.toUpperCase(), {
    x: 0.65, y: 0.42, w: 8.7, h: 0.3, align: 'left',
    fontFace: BODY, fontSize: 12, bold: true, color: ORANGE, charSpacing: 3,
  });
}

// Titulo serifado (Cambria) navy
function title(slide, text, color = NAVY_DEEP) {
  slide.addText(text, {
    x: 0.62, y: 0.72, w: 8.9, h: 0.7, align: 'left',
    fontFace: TITLE_FONT, fontSize: 27, bold: true, color,
  });
}

// Subtitulo em italico cinza
function subtitle(slide, text, y = 1.45) {
  slide.addText(text, {
    x: 0.65, y, w: 8.9, h: 0.35, align: 'left',
    fontFace: BODY, fontSize: 13, italic: true, color: MUTED,
  });
}

function bg(slide, color) { slide.background = { color }; }

// Circulo decorativo suave (como os do TP2 nos slides escuros)
function decoCircle(slide, x, y, d, color) {
  slide.addShape(pres.ShapeType.ellipse, {
    x, y, w: d, h: d, fill: { color }, line: { type: 'none' },
  });
}

const sh = () => ({ type: 'outer', color: '9BB0D6', opacity: 0.35, blur: 8, offset: 2, angle: 90 });

// Card arredondado
function card(slide, x, y, w, h, fill, lineColor) {
  slide.addShape(pres.ShapeType.roundRect, {
    x, y, w, h, rectRadius: 0.11,
    fill: { color: fill },
    line: lineColor ? { color: lineColor, width: 1 } : { type: 'none' },
    shadow: fill === CARD_LIGHT || fill === WHITE ? sh() : undefined,
  });
}

// Circulo com icone (estilo dos icones-circulo laranja do TP2)
async function iconCircle(slide, cx, cy, r, circleColor, iconName, iconColor) {
  slide.addShape(pres.ShapeType.ellipse, {
    x: cx - r, y: cy - r, w: r * 2, h: r * 2,
    fill: { color: circleColor }, line: { type: 'none' },
  });
  const isz = r * 1.05;
  slide.addImage({
    data: await icon(iconName, iconColor),
    x: cx - isz / 2, y: cy - isz / 2, w: isz, h: isz,
  });
}

// ============================================================================
// SLIDE 1 — Capa (navy)
// ============================================================================
async function slide1() {
  const s = pres.addSlide();
  bg(s, NAVY);
  decoCircle(s, 6.6, -2.2, 5.6, NAVY_DEEP);
  decoCircle(s, -1.5, 3.6, 3.6, '242E52');

  s.addImage({ data: await icon('FaLayerGroup', WHITE), x: 0.65, y: 0.62, w: 0.72, h: 0.72 });

  s.addText('TP03   —   SISTEMAS DISTRIBUÍDOS', {
    x: 0.66, y: 1.72, w: 8, h: 0.3, fontFace: BODY, fontSize: 13, bold: true,
    color: ICE, charSpacing: 3,
  });
  s.addText('Replicação e Tolerância a Falhas', {
    x: 0.6, y: 2.05, w: 9, h: 1.1, fontFace: TITLE_FONT, fontSize: 43, bold: true,
    color: WHITE,
  });
  s.addText([
    { text: 'Cluster Store ', options: { color: WHITE } },
    { text: 'primário-backup', options: { color: ORANGE, bold: true } },
    { text: '  —  o recurso R replicado em 3 nós, sobre o Cluster Sync do TP2', options: { color: WHITE } },
  ], { x: 0.65, y: 3.18, w: 9, h: 0.4, fontFace: BODY, fontSize: 16 });

  s.addShape(pres.ShapeType.line, {
    x: 0.66, y: 3.95, w: 8.7, h: 0, line: { color: '3B4590', width: 1 },
  });
  s.addText(
    'Continuação do TP2: a seção crítica agora escreve de verdade em um cluster replicado, ' +
    'tolerante a quedas e omissões — rodando em Kubernetes e na nuvem AWS.',
    { x: 0.66, y: 4.15, w: 8.6, h: 0.7, fontFace: BODY, fontSize: 12.5, italic: true, color: '9AA6D4' }
  );
  s.addText('Fábio Tavares', {
    x: 0.66, y: 4.98, w: 6, h: 0.3, fontFace: BODY, fontSize: 12, color: ICE,
  });
}

// ============================================================================
// SLIDE 2 — Continuidade TP2 -> TP3 (dois cards)
// ============================================================================
async function slide2() {
  const s = pres.addSlide();
  bg(s, WHITE);
  eyebrow(s, 'Continuidade do trabalho');
  title(s, 'Do TP2 ao TP3: o recurso R agora é real');

  const cy = 1.55, cw = 4.05, ch = 3.5;
  // Card esquerdo (claro) — TP2
  card(s, 0.6, cy, cw, ch, CARD_LIGHT);
  await iconCircle(s, 1.15, cy + 0.62, 0.34, NAVY, 'FaLock', WHITE);
  s.addText('TP2 — Exclusão Mútua', {
    x: 1.62, y: cy + 0.32, w: 2.9, h: 0.6, fontFace: TITLE_FONT, fontSize: 16, bold: true, color: NAVY_DEEP,
  });
  s.addText(
    [
      'O Cluster Sync coordena quem acessa R, sem nó mestre',
      'Multicast ordenado (RabbitMQ R_topic) + fila F privada',
      'Na seção crítica, um sleep de 0,2–1s SIMULAVA o acesso',
      'Foco: coordenar o acesso de forma justa',
    ].map((t, i, a) => ({ text: t, options: { bullet: { indent: 15 }, breakLine: true, paraSpaceAfter: i < a.length - 1 ? 8 : 0 } })),
    { x: 0.9, y: cy + 1.2, w: cw - 0.55, h: ch - 1.4, fontFace: BODY, fontSize: 12.5, color: MUTED, valign: 'top' }
  );

  // Seta laranja
  s.addImage({ data: await icon('FaArrowRightLong', ORANGE), x: 4.72, y: cy + ch / 2 - 0.18, w: 0.56, h: 0.36 });

  // Card direito (navy) — TP3
  card(s, 5.35, cy, cw, ch, NAVY);
  await iconCircle(s, 5.9, cy + 0.62, 0.34, ORANGE, 'FaClone', WHITE);
  s.addText('TP3 — Replicação + Falhas', {
    x: 6.37, y: cy + 0.32, w: 2.9, h: 0.6, fontFace: TITLE_FONT, fontSize: 16, bold: true, color: WHITE,
  });
  s.addText(
    [
      'A seção crítica ESCREVE DE VERDADE no recurso R',
      'Novo Cluster Store: 3 réplicas em primário-backup',
      'Tolera falha por queda e por omissão de nós',
      'Foco: replicar o dado e sobreviver a falhas',
    ].map((t, i, a) => ({ text: t, options: { bullet: { indent: 15 }, breakLine: true, paraSpaceAfter: i < a.length - 1 ? 8 : 0 } })),
    { x: 5.65, y: cy + 1.2, w: cw - 0.55, h: ch - 1.4, fontFace: BODY, fontSize: 12.5, color: ICE, valign: 'top' }
  );
}

// ============================================================================
// SLIDE 3 — O problema proposto (banner + 3 cards)
// ============================================================================
async function slide3() {
  const s = pres.addSlide();
  bg(s, WHITE);
  eyebrow(s, 'O problema proposto');
  title(s, 'Replicar o recurso R e sobreviver a falhas');

  // Banner peach
  card(s, 0.6, 1.5, 8.8, 0.82, PEACH);
  await iconCircle(s, 1.12, 1.91, 0.28, ORANGE, 'FaNetworkWired', WHITE);
  s.addText(
    [
      { text: 'Sem middleware na parte nova: ', options: { bold: true, color: NAVY_DEEP } },
      { text: 'toda a comunicação do Cluster Store (escrita, replicação, PING, snapshot) é HTTP puro, feito à mão.', options: { color: '9A5A33' } },
    ],
    { x: 1.55, y: 1.5, w: 7.7, h: 0.82, fontFace: BODY, fontSize: 13, valign: 'middle' }
  );

  const cards = [
    ['FaDatabase', '3 réplicas do Store', 'O recurso R vive em 3 nós. Qualquer réplica aceita a escrita — se não for a primária, repassa ao primário.'],
    ['FaDice', 'Escolha aleatória', 'Cada requisição sorteia o recurso (R1–R5) e cada acesso sorteia a réplica do Store — ponto extra da proposta.'],
    ['FaHeartbeat', 'Queda e omissão', 'PINGs periódicos + timeout detectam nós mortos. O sistema continua servindo e se recupera sozinho.'],
  ];
  const cw = 2.8, gap = 0.2, x0 = 0.6, cy = 2.62, ch = 2.65;
  for (let i = 0; i < 3; i++) {
    const x = x0 + i * (cw + gap);
    card(s, x, cy, cw, ch, NAVY);
    await iconCircle(s, x + cw / 2, cy + 0.62, 0.36, ORANGE, cards[i][0], WHITE);
    s.addText(cards[i][1], {
      x: x + 0.15, y: cy + 1.12, w: cw - 0.3, h: 0.4, align: 'center',
      fontFace: TITLE_FONT, fontSize: 15.5, bold: true, color: WHITE,
    });
    s.addText(cards[i][2], {
      x: x + 0.24, y: cy + 1.55, w: cw - 0.48, h: 1.0, align: 'center',
      fontFace: BODY, fontSize: 11.5, color: ICE,
    });
  }
}

// ============================================================================
// SLIDE 4 — Arquitetura (diagrama dois clusters)
// ============================================================================
async function slide4() {
  const s = pres.addSlide();
  bg(s, WHITE);
  eyebrow(s, 'Visão geral da solução');
  s.addText('Arquitetura: dois clusters e o broker', {
    x: 0.62, y: 0.68, w: 8.9, h: 0.55, fontFace: TITLE_FONT, fontSize: 25, bold: true, color: NAVY_DEEP,
  });

  const storeX = [2.5, 5.0, 7.5], storeY = 1.74, storeR = 0.30;
  const syncX = [1.15, 3.075, 5.0, 6.925, 8.85];
  const syncY = 3.18, syncR = 0.28, clientY = 4.72, clientR = 0.25;
  const busY = 2.46, brY = 3.78, brH = 0.4;

  // Rótulo de zona (STORE) — logo abaixo do título, acima dos círculos
  s.addText('CLUSTER STORE — 3 réplicas · primário-backup · HTTP puro', {
    x: 0.6, y: 1.14, w: 8.8, h: 0.2, align: 'center', fontFace: BODY, fontSize: 10.5, bold: true, color: MUTED, charSpacing: 1,
  });

  // Malha de replicacao (linhas tracejadas entre stores)
  for (let a = 0; a < 3; a++) for (let b = a + 1; b < 3; b++) {
    s.addShape(pres.ShapeType.line, {
      x: storeX[a], y: storeY, w: storeX[b] - storeX[a], h: 0,
      line: { color: 'A9BBE0', width: 1, dashType: 'dash' },
    });
  }
  // Conectores verticais store -> barramento
  storeX.forEach(x => s.addShape(pres.ShapeType.line, { x, y: storeY + storeR, w: 0, h: busY - storeY - storeR, line: { color: 'C9D4EC', width: 1, dashType: 'dash' } }));

  // Stores: meio = primario laranja, com coroa como selo no canto superior direito
  for (let i = 0; i < 3; i++) {
    const primary = i === 1;
    await iconCircle(s, storeX[i], storeY, storeR, primary ? ORANGE : NAVY, 'FaDatabase', WHITE);
    if (primary) s.addImage({ data: await icon('FaCrown', ORANGE), x: storeX[i] + 0.13, y: storeY - 0.28, w: 0.26, h: 0.26 });
    s.addText(primary ? `Store ${i + 1}  (primário)` : `Store ${i + 1}`, {
      x: storeX[i] - 1.0, y: storeY + storeR + 0.04, w: 2.0, h: 0.2, align: 'center',
      fontFace: BODY, fontSize: 10, bold: primary, color: primary ? ORANGE : NAVY_DEEP,
    });
  }

  // Legenda do barramento (acima da linha) + barramento HTTP
  s.addText('barramento HTTP — escrita na réplica sorteada (timeout + retentativa)', {
    x: 1.0, y: busY - 0.19, w: 8.0, h: 0.16, align: 'center', fontFace: BODY, fontSize: 9, italic: true, color: MUTED,
  });
  s.addShape(pres.ShapeType.line, { x: 1.0, y: busY, w: 8.0, h: 0, line: { color: '7DA0D8', width: 1.75 } });
  // conectores barramento -> sync
  syncX.forEach(x => s.addShape(pres.ShapeType.line, { x, y: busY, w: 0, h: syncY - syncR - busY, line: { color: 'C9D4EC', width: 1, dashType: 'dash' } }));

  // Rótulo de zona (SYNC)
  s.addText('CLUSTER SYNC — exclusão mútua (herança do TP2)', {
    x: 0.6, y: busY + 0.12, w: 8.8, h: 0.2, align: 'center', fontFace: BODY, fontSize: 10.5, bold: true, color: MUTED, charSpacing: 1,
  });

  // Sync nodes
  for (let i = 0; i < 5; i++) {
    await iconCircle(s, syncX[i], syncY, syncR, NAVY, 'FaServer', WHITE);
    s.addText(`Nó ${i + 1}`, { x: syncX[i] - 0.6, y: syncY + syncR + 0.03, w: 1.2, h: 0.2, align: 'center', fontFace: BODY, fontSize: 9.5, color: NAVY_DEEP });
  }

  // Broker bar
  card(s, 1.7, brY, 6.6, brH, NAVY);
  s.addImage({ data: await icon('FaExchangeAlt', WHITE), x: 2.0, y: brY + 0.09, w: 0.22, h: 0.22 });
  s.addText('RabbitMQ  —  Exchange fanout "R_topic"  (ACQUIRE / RELEASE)', {
    x: 2.3, y: brY, w: 5.9, h: brH, align: 'left', fontFace: BODY, fontSize: 12, bold: true, color: WHITE, valign: 'middle',
  });
  // sync -> broker connectors
  syncX.forEach(x => s.addShape(pres.ShapeType.line, { x, y: syncY + syncR, w: 0, h: brY - syncY - syncR, line: { color: 'C9D4EC', width: 1, dashType: 'dash' } }));
  // broker -> clients
  syncX.forEach(x => s.addShape(pres.ShapeType.line, { x, y: brY + brH, w: 0, h: clientY - clientR - brY - brH, line: { color: 'C9D4EC', width: 1, dashType: 'dash' } }));

  // Clients
  for (let i = 0; i < 5; i++) {
    await iconCircle(s, syncX[i], clientY, clientR, ORANGE, 'FaUser', WHITE);
    s.addText(`Cliente ${i + 1}`, { x: syncX[i] - 0.7, y: clientY + clientR + 0.03, w: 1.4, h: 0.2, align: 'center', fontFace: BODY, fontSize: 9.5, color: MUTED });
  }
}

// ============================================================================
// SLIDE 5 — Protocolo primário-backup (6 passos numerados)
// ============================================================================
async function slide5() {
  const s = pres.addSlide();
  bg(s, WHITE);
  eyebrow(s, 'Protocolo primário-backup · Figura 7.19');
  title(s, 'Como uma escrita é replicada');

  const steps = [
    ['W1', 'Sync escreve', 'Na seção crítica, sorteia uma réplica do Store e envia a escrita via HTTP.'],
    ['W2', 'Repassa ao primário', 'Se a réplica sorteada não é a primária, ela encaminha o pedido ao primário.'],
    ['W3', 'Primário replica', 'O primário aplica localmente e envia a atualização a todos os backups vivos.'],
    ['W4', 'Backups confirmam', 'Cada backup aplica a escrita e devolve um ACK ao primário.'],
    ['W5', 'Commit', 'Com os ACKs, o primário responde COMMITTED (com seq e valor novo) ao Sync.'],
    ['✓', 'Resposta ao cliente', 'O Sync publica RELEASE e devolve ao cliente o valor efetivamente gravado.'],
  ];
  const cw = 2.83, ch = 1.5, gapx = 0.16, gapy = 0.18, x0 = 0.6, y0 = 1.5;
  for (let i = 0; i < 6; i++) {
    const col = i % 3, row = Math.floor(i / 3);
    const x = x0 + col * (cw + gapx), y = y0 + row * (ch + gapy);
    card(s, x, y, cw, ch, CARD_LIGHT);
    // circulo laranja com codigo do passo
    s.addShape(pres.ShapeType.ellipse, { x: x + 0.22, y: y + 0.22, w: 0.5, h: 0.5, fill: { color: ORANGE }, line: { type: 'none' } });
    s.addText(steps[i][0], { x: x + 0.22, y: y + 0.22, w: 0.5, h: 0.5, align: 'center', valign: 'middle', fontFace: BODY, fontSize: steps[i][0].length > 2 ? 12 : 13, bold: true, color: WHITE });
    s.addText(steps[i][1], { x: x + 0.82, y: y + 0.24, w: cw - 0.95, h: 0.45, fontFace: TITLE_FONT, fontSize: 14, bold: true, color: NAVY_DEEP, valign: 'middle' });
    s.addText(steps[i][2], { x: x + 0.28, y: y + 0.78, w: cw - 0.5, h: 0.6, fontFace: BODY, fontSize: 10.8, color: MUTED });
  }
}

// ============================================================================
// SLIDE 6 — Tolerância a falhas (casos 2.1 / 2.2 / 2.3)
// ============================================================================
async function slide6() {
  const s = pres.addSlide();
  bg(s, WHITE);
  eyebrow(s, 'Tolerância a falhas · opção 2 (Cluster Store)');
  title(s, 'O que acontece quando um nó do Store cai');

  const cards = [
    ['FaPowerOff', '2.1', 'Backup cai', 'Os outros detectam por timeout de PING. As escritas seguem sem ele; ao voltar, re-sincroniza o estado por snapshot.'],
    ['FaRedo', '2.2', 'Falha com pedido em andamento', 'O Sync toma timeout e retenta em outra réplica sorteada. A deduplicação por req_id impede escrita dupla.'],
    ['FaCrown', '2.3', 'Primário cai', 'Eleição por época: o menor ID vivo assume. A escrita é retentada no novo primário; o antigo volta como backup.'],
  ];
  const cw = 2.8, gap = 0.2, x0 = 0.6, cy = 1.6, ch = 3.3;
  for (let i = 0; i < 3; i++) {
    const x = x0 + i * (cw + gap);
    card(s, x, cy, cw, ch, NAVY);
    await iconCircle(s, x + cw / 2, cy + 0.66, 0.4, ORANGE, cards[i][0], WHITE);
    // etiqueta do caso
    s.addText('CASO ' + cards[i][1], {
      x: x + 0.15, y: cy + 1.2, w: cw - 0.3, h: 0.24, align: 'center', fontFace: BODY, fontSize: 10.5, bold: true, color: ORANGE, charSpacing: 2,
    });
    s.addText(cards[i][2], {
      x: x + 0.2, y: cy + 1.46, w: cw - 0.4, h: 0.66, align: 'center', fontFace: TITLE_FONT, fontSize: 15, bold: true, color: WHITE, valign: 'top',
    });
    s.addText(cards[i][3], {
      x: x + 0.26, y: cy + 2.12, w: cw - 0.52, h: 1.05, align: 'center', fontFace: BODY, fontSize: 11.5, color: ICE,
    });
  }
}

// ============================================================================
// SLIDE 7 — Execução: Docker -> Kubernetes -> AWS EC2
// ============================================================================
async function slide7() {
  const s = pres.addSlide();
  bg(s, WHITE);
  eyebrow(s, 'Execução e implantação');
  title(s, 'Rodando em Kubernetes, na nuvem AWS');

  // Pipeline horizontal: Docker Compose -> Kubernetes -> AWS EC2
  const stages = [
    ['SiDocker', '2496ED', 'Docker Compose', '15 containers (RabbitMQ, 3 Store, 5 Sync, 5 Cliente, Dashboard) sobem com um comando.'],
    ['SiKubernetes', '326CE5', 'Kubernetes', 'StatefulSets dão identidade estável e DNS por pod. kubectl delete pod → o cluster se cura sozinho.'],
    ['FaAws', 'FF9900', 'AWS EC2 (k3s)', 'Deploy na nuvem por script. Dashboard público na porta 30500 — a turma acompanha pelo navegador.'],
  ];
  const cw = 2.6, ch = 2.55, gap = 0.5, x0 = 0.6, cy = 1.62;
  for (let i = 0; i < 3; i++) {
    const x = x0 + i * (cw + gap);
    card(s, x, cy, cw, ch, WHITE, 'E3E7F3');
    await iconCircle(s, x + cw / 2, cy + 0.66, 0.42, 'F4F6FC', stages[i][0], stages[i][1]);
    s.addText(stages[i][2], { x: x + 0.15, y: cy + 1.2, w: cw - 0.3, h: 0.35, align: 'center', fontFace: TITLE_FONT, fontSize: 16, bold: true, color: NAVY_DEEP });
    s.addText(stages[i][3], { x: x + 0.22, y: cy + 1.6, w: cw - 0.44, h: 0.85, align: 'center', fontFace: BODY, fontSize: 11.3, color: MUTED });
    if (i < 2) s.addImage({ data: await icon('FaArrowRightLong', ORANGE), x: x + cw + 0.02, y: cy + ch / 2 - 0.16, w: 0.46, h: 0.32 });
  }

  // Faixa inferior: dashboard ao vivo
  card(s, 0.6, 4.45, 8.8, 0.82, NAVY);
  await iconCircle(s, 1.12, 4.86, 0.28, ORANGE, 'FaChartLine', WHITE);
  s.addText(
    [
      { text: 'Dashboard ao vivo:  ', options: { bold: true, color: WHITE } },
      { text: 'a topologia animada mostra cada mensagem do protocolo, a eleição do primário e a consistência das 3 réplicas em tempo real.', options: { color: ICE } },
    ],
    { x: 1.55, y: 4.45, w: 7.7, h: 0.82, fontFace: BODY, fontSize: 12.5, valign: 'middle' }
  );
}

// ============================================================================
// SLIDE 8 — Conclusão (navy)
// ============================================================================
async function slide8() {
  const s = pres.addSlide();
  bg(s, NAVY);
  decoCircle(s, 7.4, 2.6, 4.8, NAVY_DEEP);

  s.addImage({ data: await icon('FaCircleCheck', WHITE), x: 0.65, y: 0.55, w: 0.7, h: 0.7 });
  s.addText('CONCLUSÃO', { x: 0.66, y: 1.42, w: 6, h: 0.3, fontFace: BODY, fontSize: 13, bold: true, color: ORANGE, charSpacing: 3 });
  s.addText('Replicação real, tolerante a falhas e na nuvem', {
    x: 0.62, y: 1.74, w: 9, h: 0.8, fontFace: TITLE_FONT, fontSize: 28, bold: true, color: WHITE,
  });

  const rows = [
    ['FaDatabase', 'Replicação primário-backup', 'O recurso R vive em 3 réplicas consistentes; qualquer uma aceita a escrita e o primário propaga aos backups.'],
    ['FaShieldHalved', 'Tolerância a falhas comprovada', 'Queda e omissão são detectadas por PING; eleição, retentativa e re-sincronização mantêm o serviço no ar.'],
    ['FaCloud', 'Kubernetes + AWS', 'Todo o sistema roda em containers, com self-healing no Kubernetes e implantação pública na EC2 da AWS.'],
  ];
  let y = 2.85;
  for (const [ic, h, d] of rows) {
    s.addImage({ data: await icon(ic, ICE), x: 0.7, y: y + 0.02, w: 0.42, h: 0.42 });
    s.addText(h, { x: 1.35, y, w: 8, h: 0.36, fontFace: TITLE_FONT, fontSize: 16, bold: true, color: WHITE });
    s.addText(d, { x: 1.35, y: y + 0.38, w: 7.8, h: 0.4, fontFace: BODY, fontSize: 12, color: '9AA6D4' });
    y += 0.9;
  }
}

// ---------- Build ----------
(async () => {
  await slide1();
  await slide2();
  await slide3();
  await slide4();
  await slide5();
  await slide6();
  await slide7();
  await slide8();
  await pres.writeFile({ fileName: '../TP03_Apresentacao.pptx' });
  console.log('OK -> TP03_Apresentacao.pptx');
})().catch(e => { console.error(e); process.exit(1); });

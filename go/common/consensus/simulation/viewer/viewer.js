/*
Consensus Trace Viewer

Renders a deterministic simulation trace tick by tick so you can see node
state, WAL log contents, and message traffic between nodes.

See README.md for the trace format and usage instructions.

Design principles:
  - Node columns are recomputed each tick; nodes animate to their new position.
  - Plain HTML + SVG + JavaScript, no build step, no dependencies.
  - The viewer only visualises the trace; it never computes consensus logic.
*/

let trace
let traceName = ""
let tickIndex = 0
let playing = false

const svg = document.getElementById("canvas")

const columnX = {
  coordinator: 80,
  primary:     320,  // poolers that hold or have held the primary role
  pooler:      560,  // replica/observer poolers
  // legacy labels from older Raft-style traces
  leader: 350,
  replica: 650,
}

const nodePositions = {}
let prevNodePositions = {}

const nodeWidth = 180
const nodeHeight = 100
const logWidth = 30
const logHeight = 20
const LOG_COLS = 5

const roleColors = {
  unprovisioned: "#e8e8e8",
  observer:      "#dbeeff",
  member:        "#f2f2f2",
  // legacy Raft roles
  leader:        "#f2f2f2",
  follower:      "#f2f2f2",
  candidate:     "#fff3cd",
}

function updateHash() {
  location.hash = encodeURIComponent(traceName) + "/" + (tickIndex + 1)
}

function loadExample(name, tick = 0) {
  trace = window.EXAMPLES[name]
  traceName = name
  tickIndex = tick
  prevNodePositions = {}
  updateHash()
  renderTick()
  renderLegend()
}

async function init() {
  const sel = document.getElementById("exampleSelect")
  for (const name of Object.keys(window.EXAMPLES || {})) {
    const opt = document.createElement("option")
    opt.value = name
    opt.textContent = name
    sel.appendChild(opt)
  }

  const hash = decodeURIComponent(location.hash.slice(1))
  const slash = hash.lastIndexOf("/")
  const exName = slash >= 0 ? hash.slice(0, slash) : hash
  const tickNum = slash >= 0 ? parseInt(hash.slice(slash + 1), 10) : 1

  const start = (exName && window.EXAMPLES[exName]) ? exName : sel.options[0]?.value
  if (start) {
    sel.value = start
    const tick = (window.EXAMPLES[exName] && tickNum >= 1) ? tickNum - 1 : 0
    loadExample(start, tick)
  }
}

function isLeaderNode(node) {
  const log = node.log || []
  const committedIdx = node.committed_through != null
    ? log.indexOf(node.committed_through)
    : log.length - 1
  const entryTypes = trace.log_entry_types || {}
  const termDetails = trace.term_details    || {}

  // Walk back through committed entries to find the most recent rule.
  for (let i = committedIdx; i >= 0; i--) {
    const entry = log[i]
    if (entryTypes[entry] === "rule") {
      const m = (termDetails[entry] || "").match(/leader=([^,\s]+)/)
      return m != null && m[1] === node.id
    }
  }
  return false
}

function computeNodeLayout(nodes) {
  const columns = {}
  for (const n of nodes) {
    const col = (n.type === "pooler" && isLeaderNode(n)) ? "primary" : n.type
    if (!columns[col]) columns[col] = []
    columns[col].push(n)
  }

  for (const col in columns) {
    columns[col].sort((a, b) => a.id.localeCompare(b.id))
    columns[col].forEach((node, i) => {
      nodePositions[node.id] = { x: columnX[col] ?? columnX.pooler, y: 80 + i * 150 }
    })
  }
}

function animateNodeMove(g, nodeId) {
  const prev = prevNodePositions[nodeId]
  const curr = nodePositions[nodeId]
  if (!prev || !curr || (prev.x === curr.x && prev.y === curr.y)) return

  const anim = svgElem("animateTransform")
  anim.setAttribute("attributeName",  "transform")
  anim.setAttribute("attributeType",  "XML")
  anim.setAttribute("type",           "translate")
  anim.setAttribute("from",           `${prev.x - curr.x} ${prev.y - curr.y}`)
  anim.setAttribute("to",             "0 0")
  anim.setAttribute("dur",            "0.4s")
  anim.setAttribute("begin",          "indefinite")
  anim.setAttribute("calcMode",       "spline")
  anim.setAttribute("keySplines",     "0.25 0.1 0.25 1")
  anim.setAttribute("fill",           "freeze")
  g.appendChild(anim)
  anim.beginElement()
}

function clearCanvas() {
  while (svg.firstChild) svg.removeChild(svg.firstChild)
}

function renderTick() {
  const tick = trace.ticks[tickIndex]

  document.getElementById("tickLabel").textContent =
    `${tickIndex + 1} / ${trace.ticks.length}`

  prevNodePositions = {...nodePositions}
  computeNodeLayout(tick.nodes)
  clearCanvas()
  addArrowheadDefs()

  renderNodes(tick.nodes)

  for (const msg of (tick.messages || [])) {
    renderMessage(msg, tick.nodes, tick.tick)
  }

  // backward compat: old-style events array
  for (const ev of (tick.events || [])) {
    if (ev.type === "send_message") drawArrow(ev.from, ev.to, ev.label, false)
  }
}

function renderNodes(nodes) {
  for (const node of nodes) {
    if (node.type === "coordinator") {
      renderCoordinator(node)
    } else {
      renderPooler(node)
    }
  }
}

function renderCoordinator(node) {
  const pos = nodePositions[node.id]
  const knownPoolers = node.known_poolers || []
  const noteExtra = node.note ? 14 : 0
  const height = Math.max(nodeHeight, 64 + knownPoolers.length * 14 + noteExtra)
  const fill = node.note === "crashed" ? "#e8e8e8" : "#fff8e1"

  const g = svgG()

  appendRect(g, pos.x, pos.y, nodeWidth, height, fill, "black")
  appendText(g, pos.x + nodeWidth / 2, pos.y + 16, node.id, 13, "middle", "bold")

  if (node.note) {
    appendText(g, pos.x + nodeWidth / 2, pos.y + 30, node.note, 9, "middle", null, "#888", "italic")
  }

  let y = pos.y + (node.note ? 44 : 32)
  appendText(g, pos.x + 6, y, `quorum_term: ${node.quorum_term_seq ?? "–"}`, 10)
  y += 14
  appendText(g, pos.x + 6, y, `primary: ${node.quorum_primary ?? "–"}`, 10)
  y += 16

  if (knownPoolers.length > 0) {
    appendText(g, pos.x + 6, y, "known poolers:", 9, null, null, "#888")
    y += 12
    for (const kp of knownPoolers) {
      const termStr = kp.term_seq != null ? ` t=${kp.term_seq}` : ""
      appendText(g, pos.x + 12, y, `${kp.id}: ${kp.role}${termStr}`, 9)
      y += 12
    }
  }

  svg.appendChild(g)
  animateNodeMove(g, node.id)
}

function renderPooler(node) {
  const pos = nodePositions[node.id]
  const logCount = (node.log || []).length
  const logRows = Math.ceil(logCount / LOG_COLS)
  const logAreaHeight = logRows > 0 ? logRows * (logHeight + 2) + 4 : 0
  const commitmentHeight = node.commitment ? 12 : 0
  const height = Math.max(nodeHeight, 42 + commitmentHeight + logAreaHeight)

  const g = svgG()

  appendRect(g, pos.x, pos.y, nodeWidth, height, roleColors[node.role] || "#f2f2f2", "black")

  const termStr = node.term_seq ? ` t=${node.term_seq}` : ""
  const roleStr = node.role ? ` (${node.role}${termStr})` : ""
  appendText(g, pos.x + nodeWidth / 2, pos.y + 16, `${node.id}${roleStr}`, 12, "middle", "bold")

  let contentY = pos.y + 28
  if (node.note) {
    appendText(g, pos.x + nodeWidth / 2, contentY, node.note, 9, "middle", null, "#666", "italic")
    contentY += 12
  }

  if (node.commitment) {
    const c = node.commitment
    appendText(g, pos.x + 6, contentY, `commit: ${c.coord} [${c.at_seq}→${c.proposed_seq}]`, 9, null, null, "#b05800")
    contentY += 12
  }

  renderLog(g, node, pos, contentY + 4)
  svg.appendChild(g)
  animateNodeMove(g, node.id)
}

function renderLog(g, node, pos, logStartY) {
  const log = node.log || []
  const termDetails   = trace.term_details    || {}
  const entryTypes    = trace.log_entry_types || {}
  const committedIdx  = log.indexOf(node.committed_through)
  const divergedSet   = new Set(node.diverged || [])

  log.forEach((entry, i) => {
    const col = i % LOG_COLS
    const row = Math.floor(i / LOG_COLS)
    const x = pos.x + 8 + col * logWidth
    const y = logStartY + row * (logHeight + 2)

    const rect = svgElem("rect")
    rect.setAttribute("x", x)
    rect.setAttribute("y", y)
    rect.setAttribute("width", logWidth - 1)
    rect.setAttribute("height", logHeight)
    rect.setAttribute("class", "log-entry")

    if (entryTypes[entry] === "rule") {
      rect.classList.add("rule")
    }

    if (divergedSet.has(entry)) {
      rect.classList.add("diverged")
    } else if (committedIdx >= 0 && i <= committedIdx) {
      rect.classList.add("committed")
    }

    if (termDetails[entry]) {
      const title = svgElem("title")
      title.textContent = termDetails[entry]
      rect.appendChild(title)
    }

    g.appendChild(rect)

    const text = svgElem("text")
    text.setAttribute("x", x + (logWidth - 1) / 2)
    text.setAttribute("y", y + 13)
    text.setAttribute("text-anchor", "middle")
    text.setAttribute("font-size", "8")
    text.textContent = entry
    g.appendChild(text)
  })
}

function renderMessage(msg, nodes, currentTick) {
  const recipients = expandTo(msg.to, nodes)
  const droppedSet = new Set(
    msg.dropped === true       ? recipients :
    Array.isArray(msg.dropped) ? msg.dropped : []
  )

  for (const toId of recipients) {
    const dropped = droppedSet.has(toId)
    let label = msg.label
    if (!dropped && msg.sent_tick != null) {
      label = `${msg.label} (+${currentTick - msg.sent_tick})`
    }
    drawArrow(msg.from, toId, label, dropped)
  }
}

function expandTo(toField, nodes) {
  if (!toField) return []
  if (toField === "*") return nodes.map(n => n.id)
  if (toField === "poolers") return nodes
    .filter(n => n.type === "pooler" || n.type === "leader" || n.type === "replica")
    .map(n => n.id)
  if (toField === "coordinators") return nodes
    .filter(n => n.type === "coordinator")
    .map(n => n.id)
  if (Array.isArray(toField)) return toField
  return [toField]
}

function drawArrow(fromId, toId, label, dropped) {
  const from = nodePositions[fromId]
  const to   = nodePositions[toId]
  if (!from || !to) return

  const startX = from.x + (from.x < to.x ? nodeWidth : 0)
  const endX   = to.x   + (to.x > from.x ? 0 : nodeWidth)
  const fromY  = from.y + nodeHeight / 2
  const toY    = to.y   + nodeHeight / 2

  const line = svgElem("line")
  line.setAttribute("x1", startX)
  line.setAttribute("y1", fromY)
  line.setAttribute("x2", endX)
  line.setAttribute("y2", toY)
  line.setAttribute("marker-end", "url(#arrow)")
  line.setAttribute("class", dropped ? "message dropped" : "message")
  svg.appendChild(line)

  const labelEl = svgElem("text")
  labelEl.setAttribute("x", (startX + endX) / 2)
  labelEl.setAttribute("y", (fromY + toY) / 2 - 6)
  labelEl.setAttribute("text-anchor", "middle")
  labelEl.setAttribute("font-size", "10")
  if (dropped) labelEl.setAttribute("fill", "#aaa")
  labelEl.textContent = label
  svg.appendChild(labelEl)
}

// --- SVG helpers ---

function svgElem(tag) {
  return document.createElementNS("http://www.w3.org/2000/svg", tag)
}

function svgG() {
  return svgElem("g")
}

function appendRect(parent, x, y, w, h, fill, stroke) {
  const rect = svgElem("rect")
  rect.setAttribute("x", x)
  rect.setAttribute("y", y)
  rect.setAttribute("width", w)
  rect.setAttribute("height", h)
  if (fill)   rect.style.fill   = fill
  if (stroke) rect.style.stroke = stroke
  parent.appendChild(rect)
  return rect
}

function appendText(parent, x, y, text, size, anchor, weight, fill, style) {
  const el = svgElem("text")
  el.setAttribute("x", x)
  el.setAttribute("y", y)
  if (size)   el.setAttribute("font-size",   size)
  if (anchor) el.setAttribute("text-anchor", anchor)
  if (weight) el.setAttribute("font-weight", weight)
  if (fill)   el.setAttribute("fill",        fill)
  if (style)  el.setAttribute("font-style",  style)
  el.textContent = text
  parent.appendChild(el)
  return el
}

function addArrowheadDefs() {
  const defs   = svgElem("defs")
  const marker = svgElem("marker")
  marker.setAttribute("id",          "arrow")
  marker.setAttribute("markerWidth",  "10")
  marker.setAttribute("markerHeight", "10")
  marker.setAttribute("refX",         "10")
  marker.setAttribute("refY",         "3")
  marker.setAttribute("orient",       "auto")
  marker.setAttribute("markerUnits",  "strokeWidth")

  const path = svgElem("path")
  path.setAttribute("d",    "M0,0 L10,3 L0,6 Z")
  path.setAttribute("fill", "red")
  marker.appendChild(path)
  defs.appendChild(marker)
  svg.appendChild(defs)
}

function nextTick() {
  if (tickIndex < trace.ticks.length - 1) {
    tickIndex++
    renderTick()
    updateHash()
  }
}

function resetTick() {
  tickIndex = 0
  renderTick()
  updateHash()
}

function prevTick() {
  if (tickIndex > 0) {
    tickIndex--
    renderTick()
    updateHash()
  }
}

function togglePlay() {
  playing = !playing
  document.getElementById("btnPlay").textContent = playing ? "⏸" : "▶"
  if (playing) playLoop()
}

async function playLoop() {
  while (playing && tickIndex < trace.ticks.length - 1) {
    nextTick()
    await new Promise(r => setTimeout(r, 600))
  }
  playing = false
  document.getElementById("btnPlay").textContent = "▶"
  updateHash()
}

function renderLegend() {
  const el = document.getElementById("legend")
  el.innerHTML = ""

  const details = trace.term_details || {}
  const keys = Object.keys(details)
  if (keys.length === 0) return

  const heading = document.createElement("strong")
  heading.textContent = "Term rules"
  el.appendChild(heading)

  const table = document.createElement("table")
  table.style.cssText = "border-collapse:collapse; margin-top:6px;"

  for (const key of keys) {
    const tr = document.createElement("tr")

    const tdKey = document.createElement("td")
    tdKey.textContent = key
    tdKey.style.cssText = "padding:2px 12px 2px 0; color:#7766cc; white-space:nowrap;"

    const tdVal = document.createElement("td")
    tdVal.textContent = details[key]
    tdVal.style.cssText = "padding:2px 0; color:#444;"

    tr.appendChild(tdKey)
    tr.appendChild(tdVal)
    table.appendChild(tr)
  }

  el.appendChild(table)
}

init()

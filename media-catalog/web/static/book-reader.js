// Books reader: foliate-js renders EPUB, MOBI/AZW/AZW3, FB2 and CBZ; PDFs use the browser's viewer.
// Adapted from foliate-js's demo reader.js (MIT, see /static/foliate/LICENSE).
import './foliate/view.js'
import { createTOCView } from './foliate/ui/tree.js'

const $ = document.querySelector.bind(document)
const data = JSON.parse($('#reader-data').textContent)
const percent = new Intl.NumberFormat('en', { style: 'percent' })

const THEMES = {
    paper: { fg: '#2b241d', bg: '#fbf7ef', link: '#a0662a' },
    sepia: { fg: '#4a3a28', bg: '#f2e7d0', link: '#8a4f1d' },
    night: { fg: '#e8dccb', bg: '#15110e', link: '#d9a54a' },
}

const store = {
    get(key, fallback) {
        try { return JSON.parse(localStorage.getItem(`books-reader:${key}`)) ?? fallback } catch { return fallback }
    },
    set(key, value) {
        try { localStorage.setItem(`books-reader:${key}`, JSON.stringify(value)) } catch { /* private window */ }
    },
}
const prefs = { theme: 'paper', size: 100, flow: 'paginated', ...store.get('prefs', {}) }

const bookCSS = ({ theme, size }) => {
    const t = THEMES[theme] ?? THEMES.paper
    return `
    @namespace epub "http://www.idpf.org/2007/ops";
    html { color-scheme: ${theme === 'night' ? 'dark' : 'light'}; color: ${t.fg}; background: ${t.bg} !important;
        font-size: ${size}% !important; }
    body { background: none !important; }
    a:link, a:visited { color: ${t.link}; }
    p, li, blockquote, dd { line-height: 1.45; hyphens: auto; -webkit-hyphens: auto; widows: 2; orphans: 2; }
    [align="left"] { text-align: left; } [align="right"] { text-align: right; }
    [align="center"] { text-align: center; } [align="justify"] { text-align: justify; }
    pre { white-space: pre-wrap !important; }
    aside[epub|type~="endnote"], aside[epub|type~="footnote"], aside[epub|type~="note"],
    aside[epub|type~="rearnote"] { display: none; }
    ${theme === 'night' ? `body { color: ${t.fg} !important; }
    body *:not(img, svg, svg *, video) { color: inherit !important;
        background-color: transparent !important; border-color: ${t.fg}33 !important; }` : ''}`
}

let noteTimer
const note = text => {
    const el = $('#note')
    el.textContent = text
    el.classList.add('show')
    clearTimeout(noteTimer)
    noteTimer = setTimeout(() => el.classList.remove('show'), 5000)
}

const status = (title, detail = '', { error = false, spin = !error } = {}) => {
    const el = $('#status')
    el.hidden = false
    el.classList.toggle('error', error)
    el.innerHTML = `<div class="msg">${spin ? '<div class="spinner"></div>' : ''}<h2></h2><div></div></div>`
    el.querySelector('h2').textContent = title
    el.querySelector('h2 + div').textContent = detail
}

const applyTheme = () => {
    document.documentElement.dataset.theme = prefs.theme
    $('#theme').value = prefs.theme
    $('#layout').value = prefs.flow
}

class Reader {
    view
    #toc
    #saveTimer
    constructor(fileId) {
        this.fileId = fileId
        this.posKey = `pos:${fileId}`
    }
    restyle() {
        this.view?.renderer.setStyles?.(bookCSS(prefs))
    }
    async open(url) {
        const view = document.createElement('foliate-view')
        document.body.append(view)
        try {
            await view.open(url)
        } catch (e) {
            view.remove()
            throw e
        }
        this.view = view
        const { book } = view
        // A broken image or stylesheet inside a book should not stop the rest of it rendering.
        book.transformTarget?.addEventListener('data', ({ detail }) => {
            detail.data = Promise.resolve(detail.data).catch(e => {
                console.error(`Failed to load ${detail.name}`, e)
                return ''
            })
        })
        view.addEventListener('load', ({ detail: { doc } }) => doc.addEventListener('keydown', onKey))
        view.addEventListener('relocate', ({ detail }) => this.#onRelocate(detail))
        view.renderer.setAttribute('flow', prefs.flow)
        view.renderer.setAttribute('max-inline-size', '42em')
        this.restyle()

        const slider = $('#slider')
        slider.dir = book.dir
        slider.addEventListener('input', e => view.goToFraction(parseFloat(e.target.value)))
        const ticks = $('#ticks')
        ticks.replaceChildren(...view.getSectionFractions().map(f => Object.assign(document.createElement('option'), { value: f })))

        if (book.toc?.length) {
            this.#toc = createTOCView(book.toc, href => {
                view.goTo(href).catch(e => console.error(e))
                closeSide()
            })
            $('#toc').replaceChildren(this.#toc.element)
        } else {
            $('#toc').innerHTML = '<p style="padding: 0.5rem 0.6rem; color: var(--dim)">This book has no table of contents.</p>'
        }

        const saved = store.get(this.posKey, null)
        try {
            await view.init({ lastLocation: saved?.cfi })
        } catch (e) {
            console.warn('Saved position no longer resolves; starting at the beginning', e)
            await view.init({})
        }
        $('#status').hidden = true
    }
    #onRelocate({ fraction, location, tocItem, pageItem, cfi }) {
        const slider = $('#slider')
        slider.value = fraction
        const where = pageItem ? `p. ${pageItem.label}` : location ? `loc ${location.current + 1}/${location.total}` : ''
        $('#where').textContent = [percent.format(fraction || 0), where].filter(Boolean).join(' · ')
        slider.title = tocItem?.label ?? ''
        if (tocItem?.href) this.#toc?.setCurrentHref?.(tocItem.href)
        clearTimeout(this.#saveTimer)
        this.#saveTimer = setTimeout(() => store.set(this.posKey, { cfi, fraction, at: Date.now() }), 400)
    }
}

let reader
const onKey = e => {
    if (e.target.closest?.('select, input, textarea')) return
    const k = e.key
    if (k === 'ArrowLeft' || k === 'PageUp' || k === 'h') reader?.view?.goLeft()
    else if (k === 'ArrowRight' || k === 'PageDown' || k === 'l' || (k === ' ' && !e.shiftKey)) reader?.view?.goRight()
    else if (k === ' ' && e.shiftKey) reader?.view?.goLeft()
    else if (k === 't') toggleSide()
    else if (k === 'Escape') closeSide()
    else return
    e.preventDefault()
}

const closeSide = () => document.body.classList.remove('side-open')
const toggleSide = () => document.body.classList.toggle('side-open')

const prepare = async c => {
    const res = await fetch(`/api/books/files/${c.id}/prepare`)
    const body = await res.json().catch(() => ({}))
    if (!res.ok) throw new Error(body.error || `${res.status} ${res.statusText}`)
    return body
}

const openPDF = url => {
    document.body.classList.add('pdf')
    const frame = Object.assign(document.createElement('iframe'), { id: 'pdf', src: url, title: 'PDF' })
    document.body.append(frame)
    $('#status').hidden = true
}

const openCandidate = async c => {
    status(c.how === 'convert' ? `Converting ${c.format} for the browser…` : `Opening ${c.format}…`,
        c.how === 'convert' ? 'The first time can take a minute; after that it opens straight away.' : '')
    const { url, mode } = await prepare(c)
    if (mode === 'pdf') return openPDF(url)
    document.body.classList.add('foliate')
    reader = new Reader(c.id)
    await reader.open(url)
}

const start = async () => {
    applyTheme()
    if (!data.fileId) return
    const list = data.candidates
    const first = list.findIndex(c => c.id === data.fileId)
    // The chosen file first; if it cannot be shown, the next best formats unless the user picked this one.
    const order = data.explicit ? [list[first]] : [...list.slice(first), ...list.slice(0, first)]
    const failures = []
    for (const c of order) {
        try {
            await openCandidate(c)
            if (failures.length) note(`${failures.join('; ')} — showing ${c.format} instead`)
            if ($('#format')) $('#format').value = c.id
            return
        } catch (e) {
            console.error(e)
            failures.push(`${c.format}: ${e.message}`)
            document.body.classList.remove('foliate', 'pdf')
        }
    }
    status('Could not open this book in the browser', failures.join(' · '), { error: true })
}

$('#format')?.addEventListener('change', e => {
    const params = new URLSearchParams(location.search)
    params.set('file', e.target.value)
    location.search = params
})
$('#theme').addEventListener('change', e => {
    prefs.theme = e.target.value
    store.set('prefs', prefs)
    applyTheme()
    reader?.restyle()
})
$('#layout').addEventListener('change', e => {
    prefs.flow = e.target.value
    store.set('prefs', prefs)
    reader?.view?.renderer.setAttribute('flow', prefs.flow)
})
const resize = step => {
    prefs.size = Math.min(200, Math.max(70, prefs.size + step))
    store.set('prefs', prefs)
    reader?.restyle()
    note(`Text size ${prefs.size}%`)
}
$('#smaller').addEventListener('click', () => resize(-10))
$('#larger').addEventListener('click', () => resize(10))
$('#prev').addEventListener('click', () => reader?.view?.goLeft())
$('#next').addEventListener('click', () => reader?.view?.goRight())
$('#toc-button').addEventListener('click', toggleSide)
$('#dim').addEventListener('click', closeSide)
document.addEventListener('keydown', onKey)

start()

# Framework Discussion - 2025-11-04

**Decision:** Stay simple with vanilla JavaScript + localStorage

---

## The Question

As the site grows (currently 3 pages: Buoys, Tides, Forecasts), we're hitting common pain points:
- **Duplication:** Nav/header/footer repeated across pages
- **State management:** Need dismissible warnings that remember across pages
- **Future growth:** More pages = more maintenance

**Should we adopt a framework?**

---

## Options Evaluated

### ✅ **Option 1: Vanilla JS + localStorage** (CHOSEN)

**What:**
- Use localStorage to persist dismissed warnings
- Keep current static HTML approach
- Simple JS for state management

**Pros:**
- ✅ No build step
- ✅ No dependencies
- ✅ Fast, lightweight
- ✅ Works with current Caddy setup
- ✅ Easy to understand/maintain

**Cons:**
- ❌ HTML duplication remains
- ❌ Manual state management
- ❌ No reactivity

**Time to implement:** ~1 hour
**Good for:** 3-5 pages, simple needs
**Decision:** **Implemented** ✅

---

### 🔵 Option 2: Alpine.js (Lightweight Framework)

**What:**
- Tiny reactive framework (15 KB)
- Vue-like syntax in HTML attributes
- No build step needed

**Example:**
```html
<div x-data="{ dismissed: false }" x-show="!dismissed">
  <button @click="dismissed = true">×</button>
</div>
```

**Pros:**
- ✅ No build step
- ✅ Reactive data binding
- ✅ Easy to learn

**Cons:**
- ❌ Still duplicate HTML structure
- ❌ New dependency to learn

**When to revisit:** If we need lots of interactivity/reactivity

---

### 🟢 Option 3: HTMX (HTML-Over-the-Wire)

**What:**
- Load HTML fragments from server
- Solves duplication elegantly

**Example:**
```html
<div hx-get="/components/nav.html" hx-trigger="load"></div>
```

**Pros:**
- ✅ No build step
- ✅ Solves duplication
- ✅ Minimal JS

**Cons:**
- ❌ Different paradigm
- ❌ Less for state management alone

**When to revisit:** If HTML duplication becomes painful (7+ pages)

---

### 🟣 Option 4: SvelteKit / Astro (Modern Framework)

**What:**
- Component-based framework
- Builds to static HTML (like current site)

**Example:**
```svelte
<Layout>
  <WarningBanner dismissible={true} />
  <ForecastCard {zone} />
</Layout>
```

**Pros:**
- ✅ Component reuse (write once)
- ✅ Type safety
- ✅ Best developer experience

**Cons:**
- ❌ Requires Node.js build step
- ❌ Learning curve
- ❌ Complex deploy

**When to revisit:** If we reach 10+ pages or build a "real app"

---

## Implementation Path (Staged Approach)

### **Phase 1: Now** ✅ (Current)
- Vanilla JS + localStorage for dismissible warnings
- Keep static HTML pages
- Deploy = file upload to Caddy

### **Phase 2: If we reach 7+ pages**
- Consider HTMX for shared components (nav/footer)
- OR simple fetch() includes pattern (no framework)

### **Phase 3: If we reach 10+ pages or complex features**
- Migrate to SvelteKit or Astro
- Still generates static files (compatible with Caddy)

---

## Decision Rationale

**Why stay simple:**
1. Current site is 3 pages (may grow to 5-7)
2. Static site is fast, simple, reliable
3. No backend = no security concerns
4. Easy to maintain/understand
5. Caddy serves files beautifully as-is

**When to reconsider:**
- Site grows to 10+ pages
- Need complex user interactions
- Team collaboration (multiple developers)
- Want TypeScript/type safety

---

## Implemented: Dismissible Warnings

**Features:**
- ✅ Click X to dismiss warning
- ✅ Dismissal persists across pages (localStorage)
- ✅ Auto-expires after 24 hours
- ✅ Per-warning tracking (can dismiss one, keep others)
- ✅ Works across all pages (Buoys, Tides, Forecasts)

**Files modified:**
- `assets/js/warning-banner.js` - Added dismiss logic
- `assets/css/warning-banner-v3.css` - Styled dismiss button

**Technical approach:**
- localStorage key: `dismissed_marine_warnings`
- Warning ID: `{zone}_{type}_{issued_time}` (unique per warning)
- Expiry: 24 hours (new warnings same day = new ID)

---

## Future Considerations

**If duplication becomes painful:**
- Simple fetch() includes for nav/footer (no framework needed)
- OR adopt HTMX for component loading

**If interactivity grows:**
- Alpine.js for reactive components
- Still no build step

**If this becomes a serious app:**
- SvelteKit/Astro for full component system
- Generates static files (same deployment model)

---

**Status:** ✅ Staying simple
**Review date:** When we hit 7-10 pages or need complex features
**Last updated:** 2025-11-04

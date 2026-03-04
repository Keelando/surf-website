# Browser State Persistence - Complete Guide

**For:** Dismissible Marine Weather Warnings
**Audience:** Developers new to client-side state management
**Last Updated:** 2025-11-04

---

## 🤔 What is "State"?

**State** = Data that needs to be remembered between page visits

**Examples:**
- ❌ "User dismissed Gale Warning" (our use case)
- ❌ Dark mode preference
- ❌ "Don't show this popup again"
- ❌ Shopping cart items
- ❌ Form data you're typing

**Without state:** Every page refresh = fresh start, no memory

---

## 📦 localStorage - Browser's Built-in Storage

### What is localStorage?

**localStorage** is a JavaScript API built into all modern browsers that lets you store data **on the user's computer**.

```javascript
// Store data
localStorage.setItem('key', 'value');

// Retrieve data
const value = localStorage.getItem('key');

// Remove data
localStorage.removeItem('key');

// Clear everything
localStorage.clear();
```

### Key Characteristics

| Feature | Details |
|---------|---------|
| **Storage Location** | User's computer (browser storage) |
| **Persistence** | Forever (until manually cleared) |
| **Capacity** | ~5-10 MB per domain |
| **Scope** | Per website (halibutbank.ca only) |
| **Privacy** | Private to user's browser |
| **Server Access** | ❌ Server cannot read it |
| **Cross-browser** | ❌ Chrome data ≠ Firefox data |
| **Cross-device** | ❌ Desktop data ≠ Mobile data |

---

## 🏗️ How Our Warning System Uses localStorage

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────┐
│                    User's Browser                        │
│  ┌───────────────────────────────────────────────────┐  │
│  │            localStorage Storage                    │  │
│  │  ┌─────────────────────────────────────────────┐  │  │
│  │  │  Key: "dismissed_marine_warnings"           │  │  │
│  │  │  Value: {                                    │  │  │
│  │  │    "strait_georgia_north_Gale_...": 1730... │  │  │
│  │  │    "strait_georgia_south_Storm_...": 1730...│  │  │
│  │  │  }                                           │  │  │
│  │  └─────────────────────────────────────────────┘  │  │
│  └───────────────────────────────────────────────────┘  │
│                                                          │
│  Page Loads → Check localStorage → Hide dismissed       │
│  User clicks X → Save to localStorage → Remove banner   │
└─────────────────────────────────────────────────────────┘

                    ↕️ Data NEVER leaves browser ↕️

┌─────────────────────────────────────────────────────────┐
│              Your Server (halibutbank.ca)                │
│  ❌ Cannot access localStorage                           │
│  ✅ Serves HTML/CSS/JS files                            │
│  ✅ Serves marine_forecast.json                         │
└─────────────────────────────────────────────────────────┘
```

---

## ⏰ How Long Data Persists

### Our Implementation: 24-Hour Expiry

```javascript
const DISMISS_DURATION_MS = 24 * 60 * 60 * 1000; // 24 hours in milliseconds

// When user dismisses warning
{
  "strait_georgia_north_Gale warning_2025-11-04T18:30:00+00:00": 1730747282341
  //                                                               ↑
  //                                              Timestamp when dismissed
}

// When checking if still dismissed
const elapsed = Date.now() - dismissedTime;
if (elapsed > DISMISS_DURATION_MS) {
  // More than 24 hours have passed → Show warning again
}
```

### Timeline Example

**Monday 10:00 AM:**
- User visits site
- Sees "Gale Warning"
- Clicks X to dismiss
- localStorage saves: `{"warning_id": 1699963200000}`

**Monday 10:00 AM - Tuesday 10:00 AM:**
- User visits site 10 times
- Warning stays hidden (localStorage remembers)

**Tuesday 10:01 AM:**
- User visits site
- Code checks: `Date.now() - 1699963200000 > 24 hours?` → YES
- Warning shows again (if still active)
- Old entry automatically deleted

---

## 🔍 Step-by-Step Code Walkthrough

### Step 1: User Dismisses Warning

```javascript
// user clicks X button
function dismissWarning(warningId) {
  // 1. Get existing dismissed warnings from localStorage
  const dismissed = JSON.parse(
    localStorage.getItem('dismissed_marine_warnings') || '{}'
  );
  // If nothing stored yet, start with empty object {}

  // 2. Add this warning to the dismissed list
  dismissed[warningId] = Date.now();
  // Example: {"strait_georgia_north_Gale_...": 1730747282341}

  // 3. Save back to localStorage
  localStorage.setItem(
    'dismissed_marine_warnings',
    JSON.stringify(dismissed)
  );

  // 4. Remove banner from page (visual feedback)
  banner.remove();
}
```

**What happens in browser:**
```
Before: localStorage['dismissed_marine_warnings'] = undefined
After:  localStorage['dismissed_marine_warnings'] = '{"warning_id":1730747282341}'
                                                     ↑
                                               JSON string
```

---

### Step 2: User Navigates to Another Page

```javascript
// Page loads (index.html, tides.html, forecasts.html)
async function displayWarningBanners() {
  // 1. Fetch warnings from server
  const data = await fetch('/data/marine_forecast.json');
  const warnings = collectActiveWarnings(data);

  // 2. Filter out dismissed warnings
  const activeWarnings = warnings.filter(warning => {
    const warningId = getWarningId(warning);
    return !isWarningDismissed(warningId); // Check localStorage
  });

  // 3. Only show non-dismissed warnings
  container.innerHTML = activeWarnings.map(createWarningBanner).join('');
}
```

**Check if dismissed:**
```javascript
function isWarningDismissed(warningId) {
  // 1. Read from localStorage
  const dismissed = JSON.parse(
    localStorage.getItem('dismissed_marine_warnings') || '{}'
  );

  // 2. Check if this warning ID exists
  const dismissedTime = dismissed[warningId];
  if (!dismissedTime) return false; // Never dismissed

  // 3. Check if expired (>24 hours)
  const elapsed = Date.now() - dismissedTime;
  if (elapsed > 24 * 60 * 60 * 1000) {
    // Expired! Clean up and return false
    delete dismissed[warningId];
    localStorage.setItem('dismissed_marine_warnings', JSON.stringify(dismissed));
    return false;
  }

  // 4. Still dismissed and not expired
  return true;
}
```

---

## 📊 Data Flow Across Pages

### Scenario 1: Dismiss on Buoys, Visit Tides

```
┌─────────────────────────────────────────────────────────┐
│ 1. User on index.html (Buoys page)                      │
│    - Sees 2 warnings (Gale + Strong Wind)               │
│    - Clicks X on Gale Warning                           │
│                                                          │
│    localStorage before: {}                               │
│    localStorage after:  {"gale_warning_id": 1730...}    │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│ 2. User navigates to tides.html                         │
│    - warning-banner.js loads                            │
│    - Reads localStorage: {"gale_warning_id": 1730...}   │
│    - Filters out Gale Warning                           │
│    - Shows ONLY Strong Wind Warning                     │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│ 3. User navigates to forecasts.html                     │
│    - warning-banner.js loads again                      │
│    - Reads SAME localStorage                            │
│    - Still filters out Gale Warning                     │
│    - Shows ONLY Strong Wind Warning                     │
└─────────────────────────────────────────────────────────┘
```

**Key point:** localStorage is **shared across all pages** on halibutbank.ca

---

## 🧹 When Data Gets Cleared

### User Actions That Clear localStorage

1. **Manual browser clear** (most common)
   ```
   Chrome: Settings → Privacy → Clear browsing data → Cookies/site data
   Firefox: Settings → Privacy → Clear Data → Cookies and Site Data
   ```

2. **Incognito/Private mode**
   - localStorage works during session
   - Cleared when window closes

3. **JavaScript command**
   ```javascript
   localStorage.clear(); // Removes ALL data
   localStorage.removeItem('dismissed_marine_warnings'); // Just ours
   ```

4. **Browser quota exceeded**
   - Rare (5-10 MB limit)
   - Browser may evict old data

### Automatic Cleanup

**Our code auto-cleans expired entries:**
```javascript
// If warning was dismissed >24 hours ago
if (elapsed > DISMISS_DURATION_MS) {
  delete dismissed[warningId]; // Remove from object
  localStorage.setItem(...);   // Save cleaned version
}
```

**Example cleanup:**
```javascript
// Before cleanup (3 warnings, 1 expired)
{
  "warning1": 1730000000000,  // ← 26 hours ago (expired)
  "warning2": 1730747282341,  // ← 2 hours ago (valid)
  "warning3": 1730750000000   // ← 1 hour ago (valid)
}

// After next page load (expired one removed)
{
  "warning2": 1730747282341,
  "warning3": 1730750000000
}
```

---

## 🔒 Privacy & Security

### What You Should Know

**✅ Good for privacy:**
- Data stays on user's computer
- Server never sees dismissals
- Each user has own private storage
- Not shared between browsers/devices

**❌ Not secure for sensitive data:**
- Any JavaScript can read it
- Not encrypted
- Browser extensions can access it
- User can inspect/edit in DevTools

**Our use case (dismissed warnings):**
- ✅ Perfect fit - no sensitive data
- ✅ User-specific preferences
- ✅ No privacy concerns

---

## 🌐 Cross-Browser & Cross-Device Behavior

### localStorage is Isolated

```
┌─────────────────────────────────────────────────────────┐
│                    User's Desktop                        │
│  ┌──────────────┐        ┌──────────────┐              │
│  │    Chrome    │        │   Firefox    │              │
│  │ localStorage │   ≠    │ localStorage │              │
│  │ {warning1}   │        │    empty     │              │
│  └──────────────┘        └──────────────┘              │
└─────────────────────────────────────────────────────────┘
                          ↕️ Different storage
┌─────────────────────────────────────────────────────────┐
│                   User's Phone                           │
│  ┌──────────────┐                                       │
│  │  Chrome App  │                                       │
│  │ localStorage │   ≠   Desktop Chrome                  │
│  │    empty     │                                       │
│  └──────────────┘                                       │
└─────────────────────────────────────────────────────────┘
```

**Result:** If user dismisses warning on desktop Chrome, they'll see it again on:
- Desktop Firefox
- Mobile Chrome
- Tablet Safari
- Different computer

---

## 🧪 Testing & Debugging

### View localStorage in Browser DevTools

**Chrome/Edge:**
1. F12 → Application tab
2. Storage → Local Storage
3. Click "halibutbank.ca"
4. See: `dismissed_marine_warnings`

**Firefox:**
1. F12 → Storage tab
2. Local Storage → halibutbank.ca
3. See stored data

**Safari:**
1. Develop → Show Web Inspector
2. Storage tab → Local Storage

### Manual Testing Commands

```javascript
// In browser console:

// 1. See current dismissals
JSON.parse(localStorage.getItem('dismissed_marine_warnings'))

// 2. Manually dismiss a warning
let dismissed = JSON.parse(localStorage.getItem('dismissed_marine_warnings') || '{}');
dismissed['test_warning'] = Date.now();
localStorage.setItem('dismissed_marine_warnings', JSON.stringify(dismissed));

// 3. Clear all dismissals
localStorage.removeItem('dismissed_marine_warnings');

// 4. Simulate expired dismissal (25 hours old)
let dismissed = JSON.parse(localStorage.getItem('dismissed_marine_warnings') || '{}');
dismissed['old_warning'] = Date.now() - (25 * 60 * 60 * 1000);
localStorage.setItem('dismissed_marine_warnings', JSON.stringify(dismissed));
// Refresh page - should auto-delete
```

---

## 📋 Our Implementation Summary

### Storage Schema

```javascript
{
  // Key: localStorage key (one per website)
  "dismissed_marine_warnings": {

    // Value: Object mapping warning IDs to timestamps
    "strait_georgia_north_Gale warning_2025-11-04T18:30:00+00:00": 1730747282341,
    "strait_georgia_south_Strong wind warning_2025-11-04T18:30:00+00:00": 1730750123456
    //        ↑ Warning ID (unique)                                              ↑ Timestamp when dismissed
  }
}
```

### Warning ID Format

```javascript
`${zone_key}_${warning_type}_${issued_utc}`

// Examples:
"strait_georgia_north_Gale warning_2025-11-04T18:30:00+00:00"
"strait_georgia_south_Storm warning_2025-11-05T12:00:00+00:00"
```

**Why this format?**
- **Zone:** Different zones = different warnings
- **Type:** Same zone can have multiple warning types
- **Issued time:** New warning issued = new ID (even same type)

### Expiry Logic

```javascript
const DISMISS_DURATION_MS = 24 * 60 * 60 * 1000; // 24 hours

// Dismissed timestamp
const dismissedTime = 1730747282341;

// Current time
const now = Date.now(); // 1730833682341 (24.x hours later)

// Check if expired
const elapsed = now - dismissedTime; // 86400000 ms (24 hours)
if (elapsed > DISMISS_DURATION_MS) {
  // Expired! Show warning again
}
```

---

## 🆚 localStorage vs Other Storage Options

### Comparison Table

| Storage Type | Persistence | Size Limit | Server Access | Use Case |
|--------------|-------------|------------|---------------|----------|
| **localStorage** | Forever | 5-10 MB | ❌ No | User preferences, dismissals |
| **sessionStorage** | Until tab closes | 5-10 MB | ❌ No | Temporary data within session |
| **Cookies** | Configurable | 4 KB | ✅ Yes | Authentication, server needs |
| **IndexedDB** | Forever | 50+ MB | ❌ No | Large datasets, offline apps |
| **Server Database** | Forever | Unlimited | ✅ Yes | User accounts, shared data |

**Why we chose localStorage:**
- ✅ Persists between visits (not just session)
- ✅ No server needed (simple!)
- ✅ Enough space for warning IDs
- ✅ Built-in browser API (no libraries)
- ✅ Perfect for user preferences

---

## 💡 Common Questions

### Q: What happens if localStorage is full?

**A:** Browser typically allows 5-10 MB per domain. Our data is tiny (~1-2 KB for 100 dismissals). If somehow full, oldest data gets evicted or setItem throws error (we'd handle gracefully).

### Q: Can users fake dismissals or edit the data?

**A:** Yes! Users can edit localStorage in DevTools. But this only affects them - it's their own browser. If they want to hide warnings from themselves, that's their choice.

### Q: What if user has cookies/localStorage disabled?

**A:** Warnings still work, just can't be dismissed (X button would do nothing). We could add a try/catch and show a message: "Enable cookies to dismiss warnings."

### Q: Does this sync across user's devices?

**A:** No. localStorage is per-browser, per-device. If you want sync, you need a server backend + user accounts.

### Q: How do I debug expiry logic?

**A:** Use browser console to manually set old timestamps (see "Testing & Debugging" section above).

---

## 🎓 Key Takeaways

1. **localStorage = Browser storage** (not server)
2. **Data persists forever** (until cleared or expired by our code)
3. **Per-browser, per-device** (not synced)
4. **Private to user** (server can't see it)
5. **Perfect for preferences** (not sensitive data)
6. **We use 24-hour expiry** (auto-cleanup)
7. **Easy to debug** (DevTools Application tab)

---

## 📚 Further Reading

**MDN Documentation:**
- [localStorage API](https://developer.mozilla.org/en-US/docs/Web/API/Window/localStorage)
- [Web Storage API](https://developer.mozilla.org/en-US/docs/Web/API/Web_Storage_API)
- [Client-side storage](https://developer.mozilla.org/en-US/docs/Learn/JavaScript/Client-side_web_APIs/Client-side_storage)

**Our Implementation Files:**
- `/site/assets/js/warning-banner.js` - See functions: `dismissWarning()`, `isWarningDismissed()`
- `/site/DISMISSIBLE_WARNINGS_SUMMARY.md` - Technical implementation details

---

**Last Updated:** 2025-11-04
**Questions?** Check browser DevTools → Application → Local Storage to see it in action!

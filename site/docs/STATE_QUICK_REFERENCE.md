# Browser State - Quick Reference Card

**For:** Dismissible Warning Banners
**Use:** Quick lookup for how state works

---

## 📦 What is localStorage?

**Browser's built-in storage** - saves data on user's computer

```javascript
// Save
localStorage.setItem('key', 'value');

// Read
localStorage.getItem('key');

// Delete
localStorage.removeItem('key');
```

---

## ⏰ How Long Data Lasts

| Event | Data Persists? |
|-------|----------------|
| Close browser | ✅ Yes |
| Restart computer | ✅ Yes |
| 24 hours pass | ❌ No (our code deletes it) |
| User clears browser data | ❌ No |
| Visit in different browser | ❌ No (separate storage) |
| Visit on different device | ❌ No (separate storage) |

---

## 🗂️ Our Data Structure

```javascript
// localStorage key
"dismissed_marine_warnings"

// Value (JSON object)
{
  "strait_georgia_north_Gale warning_2025-11-04T18:30:00": 1730747282341,
  //              ↑ Warning ID                                    ↑ When dismissed
}
```

---

## 🔄 How It Works Across Pages

```
User on Buoys page → Dismisses warning
                  ↓
        Saved to localStorage
                  ↓
User visits Tides page → Reads localStorage → Warning hidden
                  ↓
User visits Forecasts → Reads localStorage → Warning hidden
```

**Key:** Same localStorage across all pages on your site

---

## 🧪 Debug in Browser

**Chrome/Firefox DevTools:**
1. Press **F12**
2. **Application** tab (Chrome) or **Storage** tab (Firefox)
3. **Local Storage** → halibutbank.ca
4. See `dismissed_marine_warnings`

**Console commands:**
```javascript
// View dismissals
localStorage.getItem('dismissed_marine_warnings')

// Clear dismissals
localStorage.removeItem('dismissed_marine_warnings')
```

---

## 🔒 Privacy & Security

✅ **Good:**
- Data stays on user's computer
- Server never sees it
- Private to each user

❌ **Don't use for:**
- Passwords
- Credit cards
- Personal info

✅ **Our use (dismissed warnings):**
- Perfect fit!

---

## 📍 Where Data Lives

```
Desktop Chrome    →  localStorage (separate)
Desktop Firefox   →  localStorage (separate)
Mobile Chrome     →  localStorage (separate)
Incognito Mode    →  localStorage (deleted on close)
```

**Not synced between browsers/devices!**

---

## 🎯 Quick Facts

- **Size limit:** 5-10 MB per website
- **Our usage:** ~1-2 KB for 100 dismissals
- **Expiry:** We delete after 24 hours
- **Shared:** All pages on halibutbank.ca
- **Private:** Each user has their own storage

---

**Full Guide:** See `BROWSER_STATE_EXPLAINED.md` for complete details

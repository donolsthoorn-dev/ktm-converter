# Git vanuit Cursor → GitHub (structureel goed zetten)

## Waarom je soms geen push krijgt

- Gebruik je **HTTPS** (`https://github.com/...`) en een **Personal Access Token** als wachtwoord, dan bepaalt GitHub **welke rechten** dat token heeft (**scopes**).
- Zonder scope **`workflow`** mag dat token **geen** bestanden onder `.github/workflows/` wijzigen. Dan krijg je precies die fout over `workflow` scope.
- **Oplossing die je het minst hoeft na te denken:** push niet met zo’n beperkt token, maar met **SSH**. SSH heeft die “workflow scope”-limiet niet.

Cursor gebruikt gewoon je systeem-`git`. Als `git push` in de Terminal werkt, werkt **Commit / Sync** in Cursor ook.

---

## Optie A (aanbevolen): één keer SSH instellen

### 1. Maak een sleutel (als je die nog niet hebt)

In Terminal: **typ of plak alleen de onderstaande regel** — niet de `` ``` ``-tekens uit deze pagina (die zijn alleen voor opmaak in de editor).

```bash
ssh-keygen -t ed25519 -C "jouw@email.nl"
```

- Druk op Enter voor de standaard locatie (`~/.ssh/id_ed25519`).
- Wachtwoordzin mag leeg (Enter) of een korte pin; beide zijn oké.

### 2. Start ssh-agent en voeg de sleutel toe

```bash
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_ed25519
```

### 3. Publieke sleutel naar GitHub kopiëren

```bash
pbcopy < ~/.ssh/id_ed25519.pub
```

**Belangrijk:** dit is **niet** de instellingenpagina van je *repository* (`…/ktm-converter/settings`). Ook **niet** “Deploy keys” daar.

- Klik **rechtsboven** op je **profielfoto** → **Settings** (accountinstellingen).
- Linkermenu: **SSH and GPG keys** — of open direct: [github.com/settings/keys](https://github.com/settings/keys)
- **New SSH key** → titel (bijv. `MacBook`) → plakken → **Add SSH key**.

### 4. Test

```bash
ssh -T git@github.com
```

Je ziet iets als: `Hi donolsthoorn-dev! You've successfully authenticated...`

### 5. Zet deze repo op SSH (in plaats van HTTPS)

Vanaf je projectroot:

```bash
cd /Users/donolsthoorn/Documents/ktm_project
git remote set-url origin git@github.com:donolsthoorn-dev/ktm-converter.git
git remote -v
```

Je zou nu `git@github.com:donolsthoorn-dev/ktm-converter.git` moeten zien.

### 6. Push

```bash
git push origin main
```

Daarna: in Cursor gewoon **committen en Sync/Push** — het is dezelfde `origin`.

---

## Optie B: HTTPS behouden, wél een token met `workflow`

1. GitHub → **Settings → Developer settings → Personal access tokens**.
2. Nieuw token (classic): vink minimaal **`repo`** én **`workflow`** aan.
3. Oude token in **Sleutelhangslot (Keychain Access)** zoeken op `github.com` en verwijderen, of wachtwoord voor Git opnieuw laten vragen.
4. Volgende `git push`: als gebruikersnaam je GitHub-naam, als wachtwoord het **nieuwe** token.

Nadeel: je moet dit onthouden als je ooit een nieuw token maakt zonder `workflow`.

---

## Optie C: GitHub CLI (`gh`)

```bash
brew install gh
gh auth login
```

Kies GitHub.com → HTTPS → login via browser. `gh` zet credentials zo dat push meestal wél mag (inclusief workflows). Daarna nog:

```bash
git remote set-url origin https://github.com/donolsthoorn-dev/ktm-converter.git
```

(alleen nodig als je remote al goed staat — `gh` configureert vaak alles.)

---

## Job-worker workflow

Het actieve bestand staat in **`.github/workflows/job-worker.yml`** (na SSH-setup kun je dat gewoon pushen). In `docs/github-actions-job-worker.yml` staat alleen een korte verwijzing.

---

## Kort antwoord

**Structureel:** zet **`origin` op SSH** (optie A). Dan kun je vanuit Cursor blijven committen en syncen zonder steeds aan PAT-scopes te denken.

# District Intelligence Prompt for Claude Code

## How to Use

1. Open Claude Code
2. Navigate to the `modelit-district-intel` repo folder
3. Copy the entire prompt below (everything between the `---` lines)
4. Replace `[DISTRICT NAME]` with the actual district name (e.g., "Fontana Unified School District")
5. Replace `[STATE]` with the state (e.g., "California")
6. Paste into Claude Code and hit enter
7. Claude will research the district, create all files, update the spreadsheet, commit, and push

---

## THE PROMPT (copy everything below this line)

---

I need you to build a complete district intelligence profile for **[DISTRICT NAME]** in **[STATE]** for the ModelIt K12 district sales system.

## Context

You are working in the `modelit-district-intel` GitHub repo (https://github.com/charlesmartinedd/modelit-district-intel). This repo contains a district sales intelligence system for ModelIt K12, an NSF SBIR-funded computational modeling platform for K-12 science and math education. The goal is to identify school districts where ModelIt can be sold by analyzing their budgets, STEM programs, curriculum, contacts, and board meeting spending.

ModelIt K12 is:
- An NSF SBIR Phase II funded computational modeling platform
- Lets students BUILD their own systems models (not just interact with pre-built simulations)
- Bilingual (English/Spanish)
- Aligns to NGSS Science & Engineering Practice 2 (Developing and Using Models) and Practice 5 (Using Mathematics and Computational Thinking)
- Complements existing curriculum (Discovery Education, Amplify, FOSS, etc.) - does NOT replace
- Includes teacher professional development
- Best fit for grades 5-12, especially where math/science proficiency is low

## District Size & Grade Span Adjustments

For districts under 2,000 students:
- Contact tiers may collapse (superintendent may BE the curriculum director)
- CAASPP subgroup data may be suppressed (N<11)
- Fewer vendors/partners is expected
- Entry strategy should emphasize personal relationships and informal processes

For elementary-only districts (K-6 or K-8):
- CTE pathways: "N/A - elementary district"
- High school curriculum: "N/A"
- Focus on K-5 and 6-8 grade bands only

---

## What to Do

### Step 1: Research the District

Search the web for ALL of the following data points. Be thorough - use multiple sources to cross-verify. Key sources to check:

**Demographics & Academics:**
- District website (find it by searching "[DISTRICT NAME] official website")
- Ed-Data.org (for California districts): `https://www.ed-data.org/district/[COUNTY]/[DISTRICT-HYPHENATED]`
- Public School Review: `https://www.publicschoolreview.com/[STATE]/[DISTRICT-HYPHENATED]/school-district`
- CAASPP/EdSource (for California): `https://caaspp.edsource.org/sbac/[DISTRICT-NAME-HYPHENATED]-[CDS-CODE]`
- NCES (national): `https://nces.ed.gov/ccd/districtsearch/`
- Niche.com: `https://www.niche.com/k12/d/[DISTRICT-NAME-HYPHENATED]/`
- U.S. News: `https://www.usnews.com/education/k12/[STATE]/districts/[DISTRICT-NAME]`

**Collect these data points:**
- Enrollment (current year)
- Number of schools (elementary, middle, high breakdown)
- Grade span
- County
- Superintendent name and start date
- Per-pupil spending
- Total revenue
- Student-teacher ratio
- Demographics: % Hispanic, Black, White, Asian, other races
- % Economically disadvantaged
- % English Learners
- Graduation rate
- District rating/ranking
- CDS code (California) or NCES ID (national)

**Test Scores:**
- Math proficiency % (met/exceeded standards) - overall and by grade level
- ELA proficiency % - overall
- Science proficiency % - overall
- Subgroup data: Hispanic, African American, English Learners, Socioeconomically Disadvantaged
- State average comparisons
- Trend data (2-3 years if available)

**STEM & Curriculum:**
- District website STEM/science/math pages
- Science curriculum: What publisher/program do they use? By grade band (K-5, 6-8, 9-12)
- Math curriculum and intervention tools (iReady, etc.)
- STEM programs (PLTW, MESA, STEM academies, robotics, etc.)
- CTE pathways offered and at which schools
- Technology programs (CS education, 1:1 devices)
- Environmental education programs
- Any NGSS implementation details

**Budget & LCAP (California) or Strategic Plan (other states):**
- For California: Find the district's LCAP page and list all available documents with links
- For other states: Find strategic plan, budget documents, or school improvement plans
- Identify STEM-related spending categories
- Supplemental/equity funding amounts
- Title I/II/IV funding (federal)
- Any bond measures for technology

**Board Meetings:**
- Find the board agendas/minutes page URL
- Index all meetings from 2025-present with links
- Note the regular meeting schedule (day of week, time)
- Identify any known STEM/curriculum/technology agenda items from news articles

**Contacts:**
- Search the district website for staff directories, especially Education Services/Curriculum & Instruction
- Find: Superintendent, Associate/Assistant Superintendent of Instruction, STEM/Science Director/Coordinator, Math Director, Technology Director, CTE Director
- Find: Executive Assistant / Administrative Assistant to the Superintendent and/or to the Asst. Superintendent of Ed Services — these are calendar gatekeepers who control scheduling access to decision makers
- Get verified emails from the district website (not guessed)
- Find all Board of Education members with names, positions, emails, and terms
- Note the email pattern used (first.last@, finitial.last@, etc.)
- Search LinkedIn for key staff titles to fill gaps

**Vendors & Partners:**
- What curriculum publishers do they use? (check board minutes, website, SARC reports)
- What EdTech platforms? (LMS, assessment tools)
- University partnerships?
- Nonprofit/industry CTE partners?
- PD providers?

**News & Context:**
- Search for recent news about the district (leadership changes, budget issues, new programs, bond measures)
- Search for "[DISTRICT NAME] STEM" and "[DISTRICT NAME] science curriculum" and "[DISTRICT NAME] board approved contract"

### Step 2: Create District Files

Create a folder at `districts/[district-slug]/` (lowercase, hyphenated, e.g., `fontana-usd`) and create ALL of the following files. Use the templates in `_templates/` as structural guides, but fill them with real researched data:

1. **`README.md`** - Quick reference card with key metrics, primary contact, and "Why This District" bullet points (5-7 reasons ModelIt fits)

2. **`profile.md`** - Full intelligence profile including:
   - Quick facts table (all metrics with sources)
   - Demographics table
   - CAASPP/test score data (overall, by grade, by subgroup with trends)
   - STEM landscape (all programs, partnerships)
   - CTE pathways (list all with schools)
   - LCAP/budget summary (funding sources, STEM spending estimates)
   - Science curriculum summary table by grade band
   - Current vendors/partners summary
   - Problems they're solving (from LCAP/strategic plan)
   - Superintendent profile (background, start date, priorities)
   - Future readiness goals (graduation, A-G, CCI targets)
   - Data sources table with URLs and access dates

3. **`contacts.md`** - All contacts organized by tier:
   - Tier 1: Primary entry points (STEM coordinator, science director)
   - Tier 2: Decision influencers (superintendent, assistant supt.)
   - Tier 3: Curriculum staff (math director, tech director, academic agents, math strategists by grade band). **Include Executive Assistants / Administrative Assistants** to the Superintendent and/or Asst. Superintendent of Ed Services — these are calendar gatekeepers who control scheduling. Note their name, phone extension, and gatekeeper role.
   - Tier 3B: STEM support staff (community agents, partnership coordinators, secretaries who schedule STEM team meetings)
   - Tier 4: Middle school principals and assistant principals — these are PILOT SCHOOL CANDIDATES. Include ALL middle schools with principal name, email, phone, and notes on awards/recognition. Middle schools are the highest-impact deployment target because math proficiency drops most dramatically in grades 6-8.
   - Tier 4B: High school principals — especially schools with CTE, PLTW, MESA, or STEM academies
   - Tier 4C: Other education services leadership (equity directors, multilingual ed, expanded learning)
   - Tier 4D: Key school staff (program specialists, department chairs, STEM leads at target pilot schools)
   - Tier 5: Board members (all with emails, areas, terms, background notes, STEM advocacy indicators)
   - Email pattern analysis
   - Outreach priority order organized in WAVES (not just numbered):
     - Wave 1: Initial Contact (Week 1) — science TOSA + math lead + tech director
     - Wave 2: Executive Buy-In (Week 2) — asst. supt + exec directors
     - Wave 3: Curriculum Specialists (after Wave 1 response) — math strategists, STEM team
     - Wave 4: Pilot Schools (after engagement confirmed) — middle school principals, HS STEM leads
     - Wave 5: Board & Partnership (after staff engagement) — board champions, partnership coordinators
   - **MINIMUM 20 contacts total** across all tiers. Use Rialto USD (districts/rialto-usd/contacts.md) as the gold standard — ~30 contacts with full details.

4. **`board-meetings.md`** - Board meeting extraction:
   - Meeting index table (2025-present) with agenda and minutes links
   - Known key actions from news/public sources
   - **Purchasing & Warrants section**: Track vendor payments, purchase orders, contracts, and warrant registers from board consent calendars. Include extraction template for flagging: curriculum publisher payments, EdTech platform costs, technology hardware purchases, PD vendor payments, STEM equipment orders, and assessment platform costs. This reveals current vendors, contract sizes, and spending patterns.
   - Extraction template for PDF review
   - Summary statistics placeholder

5. **`lcap-extract.md`** (California) or **`budget-extract.md`** (other states):
   - Available documents with links
   - District financial context (revenue, expenditure, per-pupil)
   - Funding source estimates (LCFF, Title I/II/IV, bonds, grants)
   - LCAP goals overview with ModelIt relevance rating
   - Known STEM-related spending areas with estimates
   - Budget entry points for ModelIt (which funding source, estimated available, rationale)
   - LCAP development timeline with action dates
   - Next steps checklist for deeper extraction

6. **`science-curriculum.md`** - Science curriculum analysis:
   - Current curriculum by grade band (K-5, 6-8, 9-12) with publisher, program, adoption year
   - Supplemental programs and tools table
   - "What This Means for ModelIt" section with positioning strategy
   - Table showing how ModelIt complements (not competes with) each current tool
   - Adoption cycle opportunity analysis
   - Grade band priority based on test score data
   - Competitor presence analysis table
   - Verification needed checklist

7. **`vendors-partners.md`** - Complete vendor/partner map:
   - Curriculum publishers table
   - Technology partners table
   - CTE/industry partners table
   - Environmental/STEM partners table
   - Assessment vendors table
   - PD providers table
   - Vendor landscape analysis: where ModelIt fits, warm intro paths, competitive threats
   - Verification needed checklist

8. **`entry-strategy.md`** - Tailored sales approach:
   - Primary contact (name, title, email, phone, why this person)
   - Secondary contact
   - The Hook: 2-3 sentence opening that references SPECIFIC district data (LCAP goal, test scores, program name)
   - Budget pathway table (primary and alternative funding sources)
   - Champions to cultivate (5+ people with leverage points)
   - Decision process steps
   - 5-phase approach sequence with specific actions and timeline
   - Risks & mitigation table (6+ risks with likelihood, impact, mitigation)
   - Competitive positioning table (for each current tool, our message)
   - Success metrics for pilot
   - Key dates calendar

9. **`pitch-notes.md`** - Ready-to-use talking points:
   - Email templates for primary and secondary contacts (personalized with district data)
   - 7+ key talking points with quotable language
   - Objection handling table (7+ common objections with responses)
   - District-specific data points table (value, source, use in pitch)
   - Materials to prepare checklist

### Step 3: Update the Comparison Spreadsheet

Open `comparison.xlsx` and add a new row for this district in ALL 8 sheets:
1. District Overview - demographics and metrics
2. STEM Budget - spending categories
3. Contacts - key personnel (top 5)
4. Board Approvals - leave empty (for future extraction)
5. Vendors & Partners - all identified vendors
6. Science Curriculum - by grade band
7. Entry Strategy - hook, funding, stage
8. Pipeline - current status and next action

### Step 4: Update the Pipeline

Add the district to `pipeline.md` in the Active Pipeline table.

### Step 4.5: Self-Validate

Before committing, run through this validation checklist and output a summary table.

**File Existence Check:**
Verify all 9 files exist in `districts/[slug]/`:
- [ ] README.md
- [ ] profile.md
- [ ] contacts.md
- [ ] lcap-extract.md
- [ ] board-meetings.md
- [ ] science-curriculum.md
- [ ] vendors-partners.md
- [ ] entry-strategy.md
- [ ] pitch-notes.md

**Minimum Line Counts:**

| File | Standard (>2K students) | Small (<2K students) |
|------|------------------------|---------------------|
| README.md | 35 | 30 |
| profile.md | 150 | 100 |
| contacts.md | 200 | 100 |
| lcap-extract.md | 100 | 70 |
| board-meetings.md | 60 | 40 |
| science-curriculum.md | 70 | 50 |
| vendors-partners.md | 60 | 40 |
| entry-strategy.md | 100 | 80 |
| pitch-notes.md | 70 | 60 |

**Content Spot-Checks:**
- [ ] CDS code present in profile.md
- [ ] Enrollment sourced (with URL or document name)
- [ ] Primary contact has verified email (from district website, not guessed)
- [ ] CAASPP data included with source URL
- [ ] "The Hook" in entry-strategy.md references district-specific data (not generic)
- [ ] At least 5 objection responses in pitch-notes.md

**Cross-File Consistency:**
- [ ] Primary contact name matches across README.md, contacts.md, and entry-strategy.md
- [ ] Enrollment number matches across README.md and profile.md
- [ ] District name and slug are consistent across all files

**Repo-Level Checks:**
- [ ] comparison.xlsx updated with new district row (all 8 sheets)
- [ ] pipeline.md updated with new pipeline entry

**Output a validation summary table like this before committing:**

```
## Validation Summary: [District Name]

| Check | Status | Notes |
|-------|--------|-------|
| Files (9/9) | ✅ | All present |
| README.md (XX lines) | ✅/⚠️ | [meets/below] minimum |
| profile.md (XX lines) | ✅/⚠️ | [meets/below] minimum |
| contacts.md (XX lines) | ✅/⚠️ | [meets/below] minimum |
| lcap-extract.md (XX lines) | ✅/⚠️ | [meets/below] minimum |
| board-meetings.md (XX lines) | ✅/⚠️ | [meets/below] minimum |
| science-curriculum.md (XX lines) | ✅/⚠️ | [meets/below] minimum |
| vendors-partners.md (XX lines) | ✅/⚠️ | [meets/below] minimum |
| entry-strategy.md (XX lines) | ✅/⚠️ | [meets/below] minimum |
| pitch-notes.md (XX lines) | ✅/⚠️ | [meets/below] minimum |
| CDS code present | ✅/❌ | |
| Enrollment sourced | ✅/❌ | |
| Primary contact verified | ✅/❌ | |
| CAASPP with source | ✅/❌ | |
| Hook is district-specific | ✅/❌ | |
| 5+ objection responses | ✅/❌ | |
| Cross-file: contact name | ✅/❌ | |
| Cross-file: enrollment | ✅/❌ | |
| comparison.xlsx updated | ✅/❌ | |
| pipeline.md updated | ✅/❌ | |
```

If any check fails, fix it before proceeding to Step 5.

### Step 5: Commit and Push

Stage all new and modified files, commit with a descriptive message, and push to the repo:

```
git add districts/[district-slug]/ comparison.xlsx pipeline.md
git commit -m "Add [DISTRICT NAME] district intelligence profile

- Full profile with demographics, test scores, STEM programs
- [X] contacts identified with verified emails
- Board meetings indexed (2025-present)
- Science curriculum mapped by grade band
- [X] vendors/partners with competitive analysis
- Entry strategy targeting [PRIMARY CONTACT NAME]
- Budget pathway: [PRIMARY FUNDING SOURCE]

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"

git push
```

## Quality Standards

- **Every dollar amount and partner name must have a source** (URL or document name)
- **Only use verified emails** from district websites - do NOT guess email formats
- **Cross-reference test scores** with at least 2 sources
- **Mark unverified data as "TBD"** rather than making assumptions
- **Include "[NEEDS PDF REVIEW]"** for items requiring document download
- **Date every data point** - include "Source" and "Date Accessed" columns

## Important Notes

- This is for a UNITED STATES school district. Adapt the budget/funding analysis for the correct state:
  - **California**: Use LCAP framework, LCFF funding, CAASPP scores
  - **Texas**: Use TEA data, STAAR scores, strategic plans
  - **New York**: Use NYSED data, state assessments, budget documents
  - **Other states**: Find the equivalent accountability plan, state assessment, and funding structure
- For non-California districts, replace "LCAP" references with the state's equivalent planning document
- Always check if the district has a dedicated STEM coordinator or if science falls under a broader curriculum director
- The pitch should ALWAYS reference specific district data (test scores, program names, LCAP goals) - never be generic

---

## END OF PROMPT


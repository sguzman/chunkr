# Branding Guidelines

This template assumes every project ships with cohesive branding so the README,
docs, and UI feel intentional even before extra assets arrive. Below are the
drafted categories and artifact types we keep in sync for each new repo. When
you generate graphics (whether via ChatGPT image tools, AI art, or a designer),
cover every requested version so you never have to guess what belongs where.

## Category 1: Color SVG/PNG Marks

1. **Main colored + detailed version (hero mark)**
  - This is the “hero” image you place at the top of the main README or docs
    landing page.
  - It should feature your primary mascot, symbol, or layout (e.g., the rusty
    bed + Ferris concept) with the full color palette, subtle gradients or
    texture details, and a horizontal lockup with the project name/tagline.
  - Export as both SVG (for docs) and PNG (for README hero, marketing blurbs).

2. **Silhouette version (minimal color)**
  - A simplified variant that retains the shape of the hero imagery but strips
    it down to 1–2 flat colors (rust + charcoal, for example).
  - This version is ideal for use on low-contrast backgrounds, as a watermark,
    or as a subtle accent behind hero text.
  - Provide SVG/PNG exports without gradients and with a transparent background.

3. **Minimal favicon version**
  - Distill the mark down to a very small symbol (monogram, icon-only, single
    letter with the crab).
  - Make sure it remains legible at 32×32 or smaller.
  - Export as PNG, ideally at 32×32, 16×16, and 48×48. SVG is optional but
    helpful for high-resolution contexts.

## Category 2: Text-Based / Typographic Marks

1. **Elaborate text lockup**
  - A wordmark that pairs the project name with a tagline or descriptor.
  - Consider integrating small flourishes like a bracketed “x” or a subtle
    underline that echoes the iconography.
  - Use the brand sans or an engineered geometric typeface; keep it readable but
    distinctive.

2. **Compact/stacked text variant**
  - A condensed treatment for badges, CLI banners, or situations where
    horizontal space is limited.
  - Think of a stacked “oxbed” with the tagline below, or even a “bed” wordmark
    that works well next to ASCII art.
  - Deliver it in SVG/PNG and keep it legible at small sizes.

## General Rules

- Store generated assets under `docs/branding/` or a sensible `assets/branding`
  folder so future contributors can reuse them.
- Always pair any new artwork with a short description (captions, Markdown text)
  explaining when to use which variant.
- If you ever revisit the branding, add a short changelog entry (date + what
  changed) to this document so nothing drifts.

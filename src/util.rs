use std::path::{
  Path,
  PathBuf
};

pub fn slugify(input: &str) -> String {
  let mut out =
    String::with_capacity(input.len());
  for ch in input.chars() {
    if ch.is_ascii_alphanumeric() {
      out.push(ch.to_ascii_lowercase());
    } else if (ch.is_whitespace()
      || ch == '-'
      || ch == '_')
      && !out.ends_with('_')
    {
      out.push('_');
    }
  }
  out.trim_matches('_').to_string()
}

pub fn apply_layout(
  layout: &str,
  format: &str,
  title_slug: &str
) -> PathBuf {
  let mut rendered =
    layout.replace("{format}", format);
  rendered = rendered.replace(
    "{title_slug}",
    title_slug
  );
  PathBuf::from(rendered)
}

pub fn replace_extension(
  path: &Path,
  ext: &str
) -> PathBuf {
  let mut p = path.to_path_buf();
  p.set_extension(ext);
  p
}

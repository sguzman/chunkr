use std::collections::HashMap;

use serde_json::Value;

#[derive(Debug)]
pub struct Snapshot {
  pub title:            String,
  pub authors:          Vec<String>,
  pub publisher:        String,
  pub pubdate:          String,
  pub languages:        Vec<String>,
  pub isbn:             String,
  pub identifiers:
    HashMap<String, String>,
  pub tags:             Vec<String>,
  pub comments_present: bool,
  pub cover_present:    bool
}

fn normalize_identifiers(
  val: &Value
) -> HashMap<String, String> {
  let mut out = HashMap::new();
  if let Value::Object(map) = val {
    for (k, v) in map {
      let key = k.trim().to_lowercase();
      let val_s = v
        .as_str()
        .unwrap_or(&v.to_string())
        .trim()
        .to_string();
      if !key.is_empty()
        && !val_s.is_empty()
      {
        out.insert(key, val_s);
      }
    }
  }
  out
}

fn normalize_languages(
  val: &Value
) -> Vec<String> {
  match val {
    | Value::Null => vec![],
    | Value::Array(arr) => {
      arr
        .iter()
        .filter_map(|v| {
          v.as_str().map(|s| {
            s.trim().to_lowercase()
          })
        })
        .filter(|s| !s.is_empty())
        .collect()
    }
    | _ => {
      let raw = val
        .as_str()
        .map(|s| s.to_string())
        .unwrap_or_else(|| {
          val.to_string()
        });
      let s = raw.trim().to_lowercase();
      if s.is_empty() {
        vec![]
      } else {
        vec![s]
      }
    }
  }
}

fn normalize_tags(
  val: &Value
) -> Vec<String> {
  match val {
    | Value::Null => vec![],
    | Value::Array(arr) => {
      arr
        .iter()
        .filter_map(|v| {
          v.as_str().map(|s| {
            s.trim().to_string()
          })
        })
        .filter(|s| !s.is_empty())
        .collect()
    }
    | _ => {
      let raw = val
        .as_str()
        .map(|s| s.to_string())
        .unwrap_or_else(|| {
          val.to_string()
        });
      let s = raw.trim();
      if s.is_empty() {
        vec![]
      } else {
        s.split(',')
          .map(|x| x.trim().to_string())
          .filter(|x| !x.is_empty())
          .collect()
      }
    }
  }
}

pub fn metadata_snapshot(
  book: &Value
) -> Snapshot {
  let identifiers =
    normalize_identifiers(
      book
        .get("identifiers")
        .unwrap_or(&Value::Null)
    );
  let langs = normalize_languages(
    book
      .get("languages")
      .unwrap_or(&Value::Null)
  );

  let authors_val = book
    .get("authors")
    .unwrap_or(&Value::Null);
  let authors = match authors_val {
    | Value::Array(arr) => {
      arr
        .iter()
        .filter_map(|v| v.as_str())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
    }
    | _ => {
      let s = authors_val
        .as_str()
        .unwrap_or("")
        .trim();
      if s.is_empty() {
        vec![]
      } else {
        vec![s.to_string()]
      }
    }
  };

  Snapshot {
    title: book
      .get("title")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .trim()
      .to_string(),
    authors,
    publisher: book
      .get("publisher")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .trim()
      .to_string(),
    pubdate: book
      .get("pubdate")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .trim()
      .to_string(),
    languages: langs,
    isbn: book
      .get("isbn")
      .and_then(|v| v.as_str())
      .unwrap_or("")
      .trim()
      .to_string(),
    identifiers,
    tags: normalize_tags(
      book
        .get("tags")
        .unwrap_or(&Value::Null)
    ),
    comments_present: book
      .get("comments")
      .and_then(|v| v.as_str())
      .map(|s| !s.trim().is_empty())
      .unwrap_or(false),
    cover_present: book
      .get("cover")
      .map(|v| !v.is_null())
      .unwrap_or(false)
  }
}

pub fn score_good_enough(
  snapshot: &Snapshot,
  scoring: &crate::config::ScoringConfig
) -> (i32, Vec<String>) {
  let mut score = 0;
  let mut reasons = Vec::new();

  if !snapshot.title.is_empty() {
    score += scoring.title_weight;
  } else {
    reasons.push(
      "missing title".to_string()
    );
  }
  if !snapshot.authors.is_empty() {
    score += scoring.authors_weight;
  } else {
    reasons.push(
      "missing authors".to_string()
    );
  }
  if !snapshot.publisher.is_empty() {
    score += scoring.publisher_weight;
  } else {
    reasons.push(
      "missing publisher".to_string()
    );
  }
  if !snapshot.pubdate.is_empty() {
    score += scoring.pubdate_weight;
  } else {
    reasons.push(
      "missing pubdate".to_string()
    );
  }

  if !snapshot.isbn.is_empty() {
    score += scoring.isbn_weight;
  } else if !snapshot
    .identifiers
    .is_empty()
  {
    score += scoring.identifiers_weight;
  } else {
    reasons.push(
      "missing identifiers/isbn"
        .to_string()
    );
  }

  if !snapshot.tags.is_empty() {
    score += scoring.tags_weight;
  } else {
    reasons
      .push("missing tags".to_string());
  }

  if snapshot.comments_present {
    score += scoring.comments_weight;
  } else {
    reasons.push(
      "missing description/comments"
        .to_string()
    );
  }

  if snapshot.cover_present {
    score += scoring.cover_weight;
  } else {
    reasons.push(
      "missing cover".to_string()
    );
  }

  (score, reasons)
}

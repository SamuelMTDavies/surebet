//! Fuzzy matcher for linking Polymarket questions to bookmaker events.
//!
//! Polymarket questions look like:
//!   - "Will Arsenal win the Premier League 2025/26?"
//!   - "Arsenal vs Chelsea: Who will win?"
//!   - "Premier League Winner 2025/26" with outcomes ["Arsenal", "Liverpool", ...]
//!
//! Bookmaker events look like:
//!   - home_team: "Arsenal", away_team: "Chelsea"
//!   - outcomes: ["Arsenal", "Chelsea", "Draw"]
//!
//! We match by extracting team names from the Polymarket question and
//! comparing against the bookmaker's team names.

use crate::arb::TrackedMarket;
use crate::crossbook::fetcher::BookmakerMatch;
use tracing::debug;

/// A link between a bookmaker event and a Polymarket market.
#[derive(Debug, Clone)]
pub struct MatchLink {
    /// Index into the bookie_matches vec.
    pub bookie_idx: usize,
    /// Index into the poly_markets vec.
    pub poly_idx: usize,
    /// Confidence score (0.0-1.0).
    pub confidence: f64,
    /// Mapping of Polymarket outcome labels → bookmaker outcome names.
    /// e.g., "Yes" → "Arsenal" (for a "Will Arsenal win?" market)
    /// or "Arsenal" → "Arsenal" (for a multi-outcome market)
    pub outcome_map: Vec<(String, String)>,
}

/// Normalize a team name for comparison: lowercase, strip common suffixes,
/// collapse whitespace.
fn normalize_team(name: &str) -> String {
    let s = name.to_lowercase();
    // Strip common suffixes
    let s = s
        .replace(" fc", "")
        .replace(" cf", "")
        .replace(" sc", "")
        .replace(" afc", "")
        .replace(" city", "")
        .replace(" united", "")
        .replace(" town", "")
        .replace(" wanderers", "")
        .replace(" rovers", "");
    // Collapse whitespace
    s.split_whitespace().collect::<Vec<_>>().join(" ").trim().to_string()
}

/// Check if a string contains a team name (fuzzy).
fn contains_team(text: &str, team: &str) -> bool {
    let text_lower = text.to_lowercase();
    let team_lower = team.to_lowercase();
    let team_norm = normalize_team(team);

    // Direct substring match
    if text_lower.contains(&team_lower) {
        return true;
    }

    // Normalized match
    let text_norm = normalize_team(text);
    if text_norm.contains(&team_norm) && !team_norm.is_empty() {
        return true;
    }

    // Word-by-word: if all significant words of the team appear in the text
    let team_words: Vec<&str> = team_lower.split_whitespace().collect();
    if team_words.len() >= 2 {
        // For multi-word teams, check if all words appear
        let all_present = team_words.iter().all(|w| text_lower.contains(w));
        if all_present {
            return true;
        }
    }

    false
}

/// Score how well a Polymarket question matches a bookmaker event.
/// Returns (score, outcome_mapping) or None if no match.
fn score_match(
    bookie: &BookmakerMatch,
    poly: &TrackedMarket,
) -> Option<(f64, Vec<(String, String)>)> {
    let question = &poly.question;
    let poly_outcomes: Vec<&str> = poly.outcomes.iter().map(|(label, _)| label.as_str()).collect();

    // === Strategy 1: Binary market ("Will X win?" / "Will X beat Y?") ===
    // Match if both teams appear in the question
    let home_in_q = contains_team(question, &bookie.home_team);
    let away_in_q = contains_team(question, &bookie.away_team);

    if home_in_q && away_in_q {
        // Both teams in the question — likely a head-to-head market
        // For binary Yes/No: "Will Arsenal beat Chelsea?" → Yes=Arsenal win
        if poly_outcomes.len() == 2
            && poly_outcomes.iter().any(|o| o.eq_ignore_ascii_case("Yes"))
        {
            // Binary: figure out which team the "Yes" refers to
            // Usually the subject of the question: "Will <team> win/beat..."
            let q_lower = question.to_lowercase();
            let home_pos = q_lower.find(&bookie.home_team.to_lowercase());
            let away_pos = q_lower.find(&bookie.away_team.to_lowercase());

            // The team mentioned first (closest to "will") is the Yes team
            let yes_team = match (home_pos, away_pos) {
                (Some(h), Some(a)) if h < a => &bookie.home_team,
                (Some(_), Some(_)) => &bookie.away_team,
                (Some(_), None) => &bookie.home_team,
                (None, Some(_)) => &bookie.away_team,
                _ => return None,
            };

            // This is tricky: a binary "Will X beat Y" doesn't directly map to
            // a 3-way h2h (Home/Draw/Away). Skip for now — these are better
            // handled as multi-outcome.
            return None;
        }

        // Multi-outcome: outcomes might be team names
        let mut outcome_map = Vec::new();
        for (label, _) in &poly.outcomes {
            if contains_team(label, &bookie.home_team) {
                outcome_map.push((label.clone(), bookie.home_team.clone()));
            } else if contains_team(label, &bookie.away_team) {
                outcome_map.push((label.clone(), bookie.away_team.clone()));
            } else if label.eq_ignore_ascii_case("Draw") || label.eq_ignore_ascii_case("Tie") {
                outcome_map.push((label.clone(), "Draw".to_string()));
            }
        }

        if outcome_map.len() >= 2 {
            return Some((0.9, outcome_map));
        }
    }

    // === Strategy 2: Single team in question + multi-outcome with team names ===
    if home_in_q || away_in_q {
        let mut outcome_map = Vec::new();
        for (label, _) in &poly.outcomes {
            if contains_team(label, &bookie.home_team) {
                outcome_map.push((label.clone(), bookie.home_team.clone()));
            } else if contains_team(label, &bookie.away_team) {
                outcome_map.push((label.clone(), bookie.away_team.clone()));
            } else if label.eq_ignore_ascii_case("Draw") || label.eq_ignore_ascii_case("Tie") {
                outcome_map.push((label.clone(), "Draw".to_string()));
            }
        }

        if outcome_map.len() >= 2 {
            return Some((0.7, outcome_map));
        }
    }

    // === Strategy 3: Outcome labels directly match team names ===
    // For multi-outcome markets like "Premier League Winner" with
    // outcomes ["Arsenal", "Liverpool", "Man City", ...]
    let mut outcome_map = Vec::new();

    // Check if any Polymarket outcome matches the home or away team
    for (label, _) in &poly.outcomes {
        if contains_team(label, &bookie.home_team) {
            outcome_map.push((label.clone(), bookie.home_team.clone()));
        } else if contains_team(label, &bookie.away_team) {
            outcome_map.push((label.clone(), bookie.away_team.clone()));
        }
    }

    // Need at least 2 mapped outcomes for a meaningful comparison
    if outcome_map.len() >= 2 {
        let confidence = 0.5 + 0.1 * outcome_map.len() as f64;
        return Some((confidence.min(0.9), outcome_map));
    }

    None
}

/// Match bookmaker events to Polymarket markets. Returns the best link
/// for each bookmaker event that has a match.
pub fn match_markets(
    bookie_matches: &[BookmakerMatch],
    poly_markets: &[TrackedMarket],
) -> Vec<MatchLink> {
    let mut links = Vec::new();

    for (bi, bm) in bookie_matches.iter().enumerate() {
        let mut best_score = 0.0;
        let mut best_link: Option<MatchLink> = None;

        for (pi, pm) in poly_markets.iter().enumerate() {
            if let Some((score, outcome_map)) = score_match(bm, pm) {
                if score > best_score {
                    best_score = score;
                    best_link = Some(MatchLink {
                        bookie_idx: bi,
                        poly_idx: pi,
                        confidence: score,
                        outcome_map,
                    });
                }
            }
        }

        if let Some(link) = best_link {
            debug!(
                event = format!("{} vs {}", bm.home_team, bm.away_team),
                poly = %poly_markets[link.poly_idx].question[..poly_markets[link.poly_idx].question.len().min(40)],
                confidence = link.confidence,
                outcomes = link.outcome_map.len(),
                "matched"
            );
            links.push(link);
        }
    }

    links
}

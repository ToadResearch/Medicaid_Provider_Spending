use serde_json::Value;

use crate::geo::normalize_zip5;

#[derive(Debug, Clone)]
pub struct NpiExtract {
    pub npi: String,
    pub display_name: Option<String>,
    pub enumeration_type: Option<String>,
    pub primary_taxonomy_code: Option<String>,
    pub primary_taxonomy_desc: Option<String>,
    pub state: Option<String>,
    pub city: Option<String>,
    pub zip5: Option<String>,
}

pub fn extract_provider_fields(npi: &str, response_json: Option<&str>) -> NpiExtract {
    let mut out = NpiExtract {
        npi: npi.to_string(),
        display_name: None,
        enumeration_type: None,
        primary_taxonomy_code: None,
        primary_taxonomy_desc: None,
        state: None,
        city: None,
        zip5: None,
    };

    let Some(s) = response_json else {
        return out;
    };
    let v: Value = match serde_json::from_str(s) {
        Ok(v) => v,
        Err(_) => return out,
    };

    let results = v.get("results").and_then(|x| x.as_array());
    let Some(results) = results else {
        return out;
    };
    let Some(r0) = results.first() else {
        return out;
    };

    out.enumeration_type = r0
        .get("enumeration_type")
        .and_then(|x| x.as_str())
        .map(|s| s.to_string());

    // display_name from basic
    let basic = r0.get("basic").and_then(|x| x.as_object());
    if let Some(basic) = basic {
        if let Some(org) = basic
            .get("organization_name")
            .and_then(|x| x.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            out.display_name = Some(org.to_string());
        } else {
            let first = basic
                .get("first_name")
                .and_then(|x| x.as_str())
                .unwrap_or("");
            let middle = basic
                .get("middle_name")
                .and_then(|x| x.as_str())
                .unwrap_or("");
            let last = basic
                .get("last_name")
                .and_then(|x| x.as_str())
                .unwrap_or("");
            let mut name = String::new();
            if !first.is_empty() {
                name.push_str(first);
            }
            if !middle.is_empty() {
                if !name.is_empty() {
                    name.push(' ');
                }
                name.push_str(middle);
            }
            if !last.is_empty() {
                if !name.is_empty() {
                    name.push(' ');
                }
                name.push_str(last);
            }
            let name = name.trim();
            if !name.is_empty() {
                out.display_name = Some(name.to_string());
            }
        }
    }

    // pick LOCATION address if present
    let addresses = r0.get("addresses").and_then(|x| x.as_array());
    if let Some(addrs) = addresses {
        let mut chosen = addrs.first();
        for a in addrs {
            if a.get("address_purpose")
                .and_then(|x| x.as_str())
                .map(|s| s.eq_ignore_ascii_case("LOCATION"))
                .unwrap_or(false)
            {
                chosen = Some(a);
                break;
            }
        }
        if let Some(a) = chosen {
            out.state = a
                .get("state")
                .and_then(|x| x.as_str())
                .map(|s| s.to_string());
            out.city = a
                .get("city")
                .and_then(|x| x.as_str())
                .map(|s| s.to_string());
            out.zip5 = a
                .get("postal_code")
                .and_then(|x| x.as_str())
                .and_then(normalize_zip5);
        }
    }

    // primary taxonomy
    let taxonomies = r0.get("taxonomies").and_then(|x| x.as_array());
    if let Some(taxes) = taxonomies {
        let mut chosen = taxes.first();
        for t in taxes {
            if t.get("primary").and_then(|x| x.as_bool()).unwrap_or(false) {
                chosen = Some(t);
                break;
            }
        }
        if let Some(t) = chosen {
            out.primary_taxonomy_code = t
                .get("code")
                .and_then(|x| x.as_str())
                .map(|s| s.to_string());
            let desc = t
                .get("desc")
                .and_then(|x| x.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty());
            let group = t
                .get("taxonomy_group")
                .and_then(|x| x.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty());
            out.primary_taxonomy_desc = desc.or(group).map(|s| s.to_string());
        }
    }

    out
}

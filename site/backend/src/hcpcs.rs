use serde_json::Value;

#[derive(Debug, Clone)]
pub struct HcpcsExtract {
    pub hcpcs_code: String,
    pub short_desc: Option<String>,
    pub long_desc: Option<String>,
    pub add_dt: Option<String>,
    pub act_eff_dt: Option<String>,
    pub term_dt: Option<String>,
    pub obsolete: Option<String>,
    pub is_noc: Option<String>,
}

pub fn extract_hcpcs_fields(hcpcs_code: &str, response_json: Option<&str>) -> HcpcsExtract {
    let mut out = HcpcsExtract {
        hcpcs_code: hcpcs_code.to_string(),
        short_desc: None,
        long_desc: None,
        add_dt: None,
        act_eff_dt: None,
        term_dt: None,
        obsolete: None,
        is_noc: None,
    };

    let Some(s) = response_json else {
        return out;
    };
    let v: Value = match serde_json::from_str(s) {
        Ok(v) => v,
        Err(_) => return out,
    };
    let Some(arr) = v.as_array() else {
        return out;
    };
    let Some(extra_fields) = arr.get(2).and_then(|x| x.as_object()) else {
        return out;
    };

    out.short_desc = first_string(extra_fields.get("short_desc"));
    out.long_desc = first_string(extra_fields.get("long_desc"));
    out.add_dt = first_string(extra_fields.get("add_dt"));
    out.act_eff_dt = first_string(extra_fields.get("act_eff_dt"));
    out.term_dt = first_string(extra_fields.get("term_dt"));
    out.obsolete = first_string(extra_fields.get("obsolete"));
    out.is_noc = first_string(extra_fields.get("is_noc"));

    out
}

fn first_string(v: Option<&Value>) -> Option<String> {
    v.and_then(|x| x.as_array())
        .and_then(|a| a.first())
        .and_then(|x| x.as_str())
        .map(|s| s.to_string())
}

use crate::{
    parse_context::ParseContext, parser_support::is_keyword, CypherError, EdgeDirection,
    EdgePattern, NodePattern, Pattern, PropertyEntry,
};

impl<'a> ParseContext<'a> {
    pub(crate) fn parse_pattern(&mut self) -> Result<Pattern, CypherError> {
        let mut nodes: Vec<NodePattern> = Vec::new();
        let mut edges: Vec<EdgePattern> = Vec::new();

        loop {
            self.expect("(")?;
            nodes.push(self.parse_node_inner()?);

            loop {
                let next = self.peek();
                if next == Some("-") || next == Some("<") {
                    if let Some(edge) = self.try_parse_edge()? {
                        edges.push(edge);
                        self.expect("(")?;
                        nodes.push(self.parse_node_inner()?);
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }

            if self.peek() == Some(",") {
                self.advance();
            } else {
                break;
            }
        }

        Ok(Pattern {
            path_variable: None,
            shortest_path: false,
            nodes,
            edges,
        })
    }

    fn parse_node_inner(&mut self) -> Result<NodePattern, CypherError> {
        let mut variable: Option<String> = None;
        let mut labels: Vec<String> = Vec::new();
        let mut properties: Vec<PropertyEntry> = Vec::new();

        if let Some(t) = self.peek() {
            if t != ")" && t != ":" && t != "{" && !is_keyword(t) {
                variable = Some(self.advance().unwrap().to_string());
            }
        }

        while self.peek() == Some(":") {
            self.advance();
            let label = self.advance_identifier()?;
            labels.push(label);
        }

        if self.peek() == Some("{") {
            self.advance();
            properties = self.parse_properties_map()?;
        }

        self.expect(")")?;

        Ok(NodePattern {
            variable,
            labels,
            properties,
        })
    }

    fn try_parse_edge(&mut self) -> Result<Option<EdgePattern>, CypherError> {
        let saved_pos = self.pos;

        let prefix_incoming = if self.peek() == Some("<") {
            self.advance();
            if self.peek() != Some("-") {
                self.pos = saved_pos;
                return Ok(None);
            }
            self.advance();
            true
        } else {
            self.advance();
            false
        };

        if self.peek() != Some("[") {
            self.pos = saved_pos;
            return Ok(None);
        }
        self.advance();

        let edge = self.parse_edge_inner()?;

        self.expect("]")?;

        let suffix_arrow = if self.peek() == Some("-") {
            self.advance();
            if self.peek() == Some(">") {
                self.advance();
                true
            } else {
                false
            }
        } else {
            self.pos = saved_pos;
            return Ok(None);
        };

        let direction = if prefix_incoming && !suffix_arrow {
            EdgeDirection::Incoming
        } else if !prefix_incoming && suffix_arrow {
            EdgeDirection::Outgoing
        } else {
            EdgeDirection::Both
        };

        Ok(Some(EdgePattern { direction, ..edge }))
    }

    fn parse_edge_inner(&mut self) -> Result<EdgePattern, CypherError> {
        let mut variable: Option<String> = None;
        let mut rel_type: Option<String> = None;
        let mut properties: Vec<PropertyEntry> = Vec::new();
        let mut min_hops: Option<u32> = None;
        let mut max_hops: Option<u32> = None;

        if let Some(t) = self.peek() {
            if t != "]" && t != ":" && t != "*" && t != "{" {
                variable = Some(self.advance().unwrap().to_string());
            }
        }

        if self.peek() == Some(":") {
            self.advance();
            rel_type = Some(self.advance_identifier()?);
        }

        if self.peek() == Some("*") {
            self.advance();
            min_hops = Some(1);

            if let Some(t) = self.peek() {
                if let Ok(min) = t.parse::<u32>() {
                    self.advance();
                    min_hops = Some(min);
                    if self.peek() == Some(".") {
                        self.advance();
                        self.expect(".")?;
                        if let Some(max_t) = self.peek() {
                            if let Ok(max) = max_t.parse::<u32>() {
                                self.advance();
                                max_hops = Some(max);
                            }
                        }
                    } else {
                        max_hops = Some(min);
                    }
                } else if t == "." {
                    self.advance();
                    self.expect(".")?;
                    if let Some(max_t) = self.peek() {
                        if let Ok(max) = max_t.parse::<u32>() {
                            self.advance();
                            max_hops = Some(max);
                        }
                    }
                }
            }
        }

        if self.peek() == Some("{") {
            self.advance();
            properties = self.parse_properties_map()?;
        }

        Ok(EdgePattern {
            variable,
            rel_type,
            direction: EdgeDirection::Both,
            properties,
            min_hops,
            max_hops,
        })
    }

    fn parse_properties_map(&mut self) -> Result<Vec<PropertyEntry>, CypherError> {
        let mut entries = Vec::new();
        while self.peek() != Some("}") && self.peek().is_some() {
            let key = self.advance_identifier()?;
            self.expect(":")?;
            let val = self.parse_expression_item(&[",", "}"])?;
            entries.push(PropertyEntry { key, value: val });
            if self.peek() == Some(",") {
                self.advance();
            }
        }
        self.expect("}")?;
        Ok(entries)
    }
}

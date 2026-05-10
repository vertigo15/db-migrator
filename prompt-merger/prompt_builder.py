from typing import Optional

from prompts import SYSTEM_PROMPT, TEMPLATE_PROMPT


def _detect_target_language(*sections: Optional[str]) -> str:
    """Return a simple target language hint for the LLM."""
    text = "\n".join(section or "" for section in sections)
    hebrew_chars = sum(1 for ch in text if "\u0590" <= ch <= "\u05ff")
    latin_chars = sum(1 for ch in text if ("a" <= ch.lower() <= "z"))
    if hebrew_chars and hebrew_chars >= latin_chars:
        return "Hebrew"
    if latin_chars:
        return "English"
    return "the original source language"


def build_system_message(company_name: str, template: Optional[str] = None) -> str:
    tpl = template or TEMPLATE_PROMPT
    return f"{SYSTEM_PROMPT}\n\nTARGET TEMPLATE:\n{tpl}\n\nCOMPANY NAME: {company_name}"


def build_user_message(
    tone: Optional[str],
    guardrail: Optional[str],
    response: Optional[str],
) -> str:
    target_language = _detect_target_language(tone, guardrail, response)
    parts = [
        f"TARGET OUTPUT LANGUAGE:\n{target_language}",
        f"TONE SECTION:\n{tone or '(not provided)'}",
        f"GUARDRAIL SECTION:\n{guardrail or '(not provided)'}",
        f"RESPONSE SECTION:\n{response or '(not provided)'}",
    ]
    return (
        "\n\n".join(parts)
        + "\n\nMerge these into the template structure above. "
        + f"The entire final prompt must be written in {target_language}."
    )


def build_template_only(company_name: str, template: Optional[str] = None) -> str:
    """Return the raw template with the company name injected.

    Used for agents that have no prompt content at all.
    """
    tpl = template or TEMPLATE_PROMPT
    return tpl.replace("[Company/Brand Name]", company_name)


def build_fallback(
    tone: Optional[str],
    guardrail: Optional[str],
    response: Optional[str],
) -> str:
    """Simple concatenation fallback when the LLM call fails."""
    sections = []
    if tone and tone.strip():
        sections.append(f"[Tone]\n{tone.strip()}")
    if guardrail and guardrail.strip():
        sections.append(f"[Guardrail]\n{guardrail.strip()}")
    if response and response.strip():
        sections.append(f"[Response]\n{response.strip()}")
    return "\n\n".join(sections) if sections else ""

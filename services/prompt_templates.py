from services.llm_schema_service import PROMPT_SCHEMA_VERSION


def with_template_header(prompt_body: str) -> str:
    return f'[actorwatch_prompt_schema:{PROMPT_SCHEMA_VERSION}] {prompt_body}'

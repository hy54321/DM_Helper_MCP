from typing import List, Tuple
from mcp.types import Prompt, PromptMessage
from anthropic.types import MessageParam

from core.chat import Chat
from core.claude import Claude
from mcp_client import MCPClient


SYSTEM_PROMPT = """You are DM Helper — a data-migration assistant.

You help users compare source-system data extracts against target-ERP
extracts (CSV / Excel files) using DuckDB-powered tools.

Capabilities:
• Catalog management: scan folders, list datasets, preview data.
• Data profiling: column statistics, value distributions, duplicate detection.
• Source ↔ target comparison: ADDED/REMOVED/CHANGED analysis with XLSX reports.
• Read-only SQL queries against loaded datasets.

Rules:
1. NEVER modify source or target data files.
2. Return summaries and samples — not full datasets — unless writing to a report.
3. When comparing, always confirm key columns with the user before running.
4. For large result sets, cap at 10 rows by default; mention total count.
5. Point users to generated XLSX reports for full data.
"""


class CliChat(Chat):
    def __init__(
        self,
        dm_client: MCPClient,
        clients: dict[str, MCPClient],
        claude_service: Claude,
    ):
        super().__init__(clients=clients, claude_service=claude_service)

        self.dm_client: MCPClient = dm_client

    async def list_prompts(self) -> list[Prompt]:
        return await self.dm_client.list_prompts()

    async def list_dataset_ids(self) -> list[str]:
        """Return dataset IDs from the data://datasets resource."""
        import json

        raw = await self.dm_client.read_resource("data://datasets")
        if raw:
            try:
                data = json.loads(raw)
                return [d["id"] for d in data]
            except Exception:
                pass
        return []

    async def get_prompt(
        self, command: str, args: dict[str, str]
    ) -> list[PromptMessage]:
        return await self.dm_client.get_prompt(command, args)

    async def _process_query(self, query: str):
        if await self._process_command(query):
            return

        prompt = f"""{SYSTEM_PROMPT}

The user says:
<query>
{query}
</query>

Answer the user's question directly and concisely. Use the available tools
to look up data when needed. Start with the exact information they need.
"""
        self.messages.append({"role": "user", "content": prompt})

    async def _process_command(self, query: str) -> bool:
        if not query.startswith("/"):
            return False

        words = query.split()
        command = words[0].replace("/", "")

        # Build args dict from remaining words
        args: dict[str, str] = {}
        prompts = await self.list_prompts()
        prompt_names = {p.name for p in prompts}

        if command not in prompt_names:
            return False

        # Map positional args
        prompt_def = next((p for p in prompts if p.name == command), None)
        if prompt_def and prompt_def.arguments:
            for i, arg_def in enumerate(prompt_def.arguments):
                if i + 1 < len(words):
                    args[arg_def.name] = words[i + 1]

        messages = await self.get_prompt(command, args)
        self.messages += convert_prompt_messages_to_message_params(messages)
        return True


def convert_prompt_message_to_message_param(
    prompt_message: "PromptMessage",
) -> MessageParam:
    role = "user" if prompt_message.role == "user" else "assistant"

    content = prompt_message.content

    if isinstance(content, dict) or hasattr(content, "__dict__"):
        content_type = (
            content.get("type", None)
            if isinstance(content, dict)
            else getattr(content, "type", None)
        )
        if content_type == "text":
            content_text = (
                content.get("text", "")
                if isinstance(content, dict)
                else getattr(content, "text", "")
            )
            return {"role": role, "content": content_text}

    if isinstance(content, list):
        text_blocks = []
        for item in content:
            if isinstance(item, dict) or hasattr(item, "__dict__"):
                item_type = (
                    item.get("type", None)
                    if isinstance(item, dict)
                    else getattr(item, "type", None)
                )
                if item_type == "text":
                    item_text = (
                        item.get("text", "")
                        if isinstance(item, dict)
                        else getattr(item, "text", "")
                    )
                    text_blocks.append({"type": "text", "text": item_text})

        if text_blocks:
            return {"role": role, "content": text_blocks}

    return {"role": role, "content": ""}


def convert_prompt_messages_to_message_params(
    prompt_messages: List[PromptMessage],
) -> List[MessageParam]:
    return [
        convert_prompt_message_to_message_param(msg) for msg in prompt_messages
    ]

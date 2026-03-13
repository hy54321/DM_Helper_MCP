from typing import List, Optional
from prompt_toolkit import PromptSession
from prompt_toolkit.completion import Completer, Completion
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.styles import Style
from prompt_toolkit.history import InMemoryHistory
from prompt_toolkit.auto_suggest import AutoSuggest, Suggestion
from prompt_toolkit.document import Document
from prompt_toolkit.buffer import Buffer

from core.cli_chat import CliChat


class CommandAutoSuggest(AutoSuggest):
    def __init__(self, prompts: List):
        self.prompts = prompts
        self.prompt_dict = {prompt.name: prompt for prompt in prompts}

    def get_suggestion(
        self, buffer: Buffer, document: Document
    ) -> Optional[Suggestion]:
        text = document.text

        if not text.startswith("/"):
            return None

        parts = text[1:].split()

        if len(parts) == 1:
            cmd = parts[0]

            if cmd in self.prompt_dict:
                prompt = self.prompt_dict[cmd]
                if prompt.arguments:
                    return Suggestion(f" <{prompt.arguments[0].name}>")

        return None


class UnifiedCompleter(Completer):
    def __init__(self):
        self.prompts = []
        self.prompt_dict = {}
        self.datasets: List[str] = []

    def update_prompts(self, prompts: List):
        self.prompts = prompts
        self.prompt_dict = {prompt.name: prompt for prompt in prompts}

    def update_datasets(self, datasets: List[str]):
        self.datasets = datasets

    def get_completions(self, document, complete_event):
        text = document.text
        text_before_cursor = document.text_before_cursor

        # @ mentions for dataset IDs
        if "@" in text_before_cursor:
            last_at_pos = text_before_cursor.rfind("@")
            prefix = text_before_cursor[last_at_pos + 1 :]

            for ds_id in self.datasets:
                if ds_id.lower().startswith(prefix.lower()):
                    yield Completion(
                        ds_id,
                        start_position=-len(prefix),
                        display=ds_id,
                        display_meta="Dataset",
                    )
            return

        # / commands (prompts)
        if text.startswith("/"):
            parts = text[1:].split()

            if len(parts) <= 1 and not text.endswith(" "):
                cmd_prefix = parts[0] if parts else ""

                for prompt in self.prompts:
                    if prompt.name.startswith(cmd_prefix):
                        yield Completion(
                            prompt.name,
                            start_position=-len(cmd_prefix),
                            display=f"/{prompt.name}",
                            display_meta=prompt.description or "",
                        )
                return

            # After command name, suggest dataset IDs
            if len(parts) >= 1 and text.endswith(" "):
                for ds_id in self.datasets:
                    yield Completion(
                        ds_id,
                        start_position=0,
                        display=ds_id,
                        display_meta="Dataset",
                    )
                return

            if len(parts) >= 2:
                ds_prefix = parts[-1]
                for ds_id in self.datasets:
                    if ds_id.lower().startswith(ds_prefix.lower()):
                        yield Completion(
                            ds_id,
                            start_position=-len(ds_prefix),
                            display=ds_id,
                            display_meta="Dataset",
                        )
                return


class CliApp:
    def __init__(self, agent: CliChat):
        self.agent = agent
        self.datasets: List[str] = []
        self.prompts = []

        self.completer = UnifiedCompleter()

        self.command_autosuggester = CommandAutoSuggest([])

        self.kb = KeyBindings()

        @self.kb.add("/")
        def _(event):
            buffer = event.app.current_buffer
            if buffer.document.is_cursor_at_the_end and not buffer.text:
                buffer.insert_text("/")
                buffer.start_completion(select_first=False)
            else:
                buffer.insert_text("/")

        @self.kb.add("@")
        def _(event):
            buffer = event.app.current_buffer
            buffer.insert_text("@")
            if buffer.document.is_cursor_at_the_end:
                buffer.start_completion(select_first=False)

        @self.kb.add(" ")
        def _(event):
            buffer = event.app.current_buffer
            text = buffer.text

            buffer.insert_text(" ")

            if text.startswith("/"):
                parts = text[1:].split()

                if len(parts) == 1:
                    buffer.start_completion(select_first=False)

        self.history = InMemoryHistory()
        self.session = PromptSession(
            completer=self.completer,
            history=self.history,
            key_bindings=self.kb,
            style=Style.from_dict(
                {
                    "prompt": "#aaaaaa",
                    "completion-menu.completion": "bg:#222222 #ffffff",
                    "completion-menu.completion.current": "bg:#444444 #ffffff",
                }
            ),
            complete_while_typing=True,
            complete_in_thread=True,
            auto_suggest=self.command_autosuggester,
        )

    async def initialize(self):
        await self.refresh_datasets()
        await self.refresh_prompts()

    async def refresh_datasets(self):
        try:
            self.datasets = await self.agent.list_dataset_ids()
            self.completer.update_datasets(self.datasets)
        except Exception as e:
            print(f"Error refreshing datasets: {e}")

    async def refresh_prompts(self):
        try:
            self.prompts = await self.agent.list_prompts()
            self.completer.update_prompts(self.prompts)
            self.command_autosuggester = CommandAutoSuggest(self.prompts)
            self.session.auto_suggest = self.command_autosuggester
        except Exception as e:
            print(f"Error refreshing prompts: {e}")

    async def run(self):
        print("ProtoQuery - Data Migration Assistant")
        print("Type your query, use /command, or @dataset_id for autocomplete.")
        print("Press Ctrl+C to exit.\n")

        while True:
            try:
                user_input = await self.session.prompt_async("dm> ")
                if not user_input.strip():
                    continue

                response = await self.agent.run(user_input)
                print(f"\n{response}\n")

                # Refresh datasets after catalog-modifying commands
                lower = user_input.lower()
                if "refresh" in lower or "scan" in lower or "catalog" in lower:
                    await self.refresh_datasets()

            except KeyboardInterrupt:
                print("\nGoodbye!")
                break


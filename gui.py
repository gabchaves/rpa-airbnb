import threading

import customtkinter as ctk

import main as scraper

ctk.set_appearance_mode("Dark")
ctk.set_default_color_theme("blue")


class App(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.title("Airbnb Host Scraper")
        self.geometry("760x560")
        self.minsize(700, 520)

        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(2, weight=1)

        self.label_title = ctk.CTkLabel(self, text="Airbnb Host Scraper", font=("Segoe UI", 26, "bold"))
        self.label_title.grid(row=0, column=0, padx=20, pady=(20, 8), sticky="w")

        self.frame_config = ctk.CTkFrame(self)
        self.frame_config.grid(row=1, column=0, padx=20, pady=10, sticky="ew")
        self.frame_config.grid_columnconfigure(1, weight=1)

        self.label_city = ctk.CTkLabel(self.frame_config, text="Cidade:")
        self.label_city.grid(row=0, column=0, padx=12, pady=10, sticky="w")

        self.entry_city = ctk.CTkEntry(self.frame_config, placeholder_text="Ex: Jacarei - SP")
        self.entry_city.insert(0, "Jacarei - SP")
        self.entry_city.grid(row=0, column=1, padx=12, pady=10, sticky="ew")

        self.label_qtd = ctk.CTkLabel(self.frame_config, text="Qtd. perfis:")
        self.label_qtd.grid(row=1, column=0, padx=12, pady=10, sticky="w")

        self.entry_qtd = ctk.CTkEntry(self.frame_config, placeholder_text="Ex: 20")
        self.entry_qtd.insert(0, "10")
        self.entry_qtd.grid(row=1, column=1, padx=12, pady=10, sticky="ew")

        self.switch_headless = ctk.CTkSwitch(self.frame_config, text="Ver navegador (mais lento)")
        self.switch_headless.grid(row=2, column=0, padx=12, pady=(5, 12), sticky="w")

        self.btn_start = ctk.CTkButton(
            self.frame_config,
            text="INICIAR RASPAGEM",
            command=self.start_scraping,
            fg_color="#FF5A5F",
            hover_color="#D93B3F",
            font=("Segoe UI", 13, "bold"),
            height=38,
        )
        self.btn_start.grid(row=2, column=1, padx=12, pady=(5, 12), sticky="e")

        self.textbox_log = ctk.CTkTextbox(self, corner_radius=8)
        self.textbox_log.grid(row=2, column=0, padx=20, pady=(0, 20), sticky="nsew")
        self.textbox_log.insert("0.0", "--- Logs ---\n\n")
        self.textbox_log.configure(state="disabled")

    def log_message(self, message: str) -> None:
        self.after(0, lambda: self._update_log_ui(message))

    def _update_log_ui(self, message: str) -> None:
        try:
            self.textbox_log.configure(state="normal")
            self.textbox_log.insert("end", f"{message}\n")
            self.textbox_log.see("end")
            self.textbox_log.configure(state="disabled")
        except Exception:
            pass

    def set_running_state(self, running: bool) -> None:
        if running:
            self.btn_start.configure(state="disabled", text="RODANDO...")
        else:
            self.btn_start.configure(state="normal", text="INICIAR RASPAGEM")

    def start_scraping(self) -> None:
        city = self.entry_city.get().strip()
        qtd_str = self.entry_qtd.get().strip()

        if not city:
            self.log_message("ERRO: informe uma cidade.")
            return

        try:
            qtd = int(qtd_str) if qtd_str else 10
            if qtd <= 0:
                raise ValueError
        except ValueError:
            self.log_message("ERRO: a quantidade de perfis deve ser um numero inteiro positivo.")
            return

        is_visual = bool(self.switch_headless.get())
        headless = not is_visual

        self.set_running_state(True)
        self.log_message(f"Iniciando raspagem: cidade='{city}', perfis={qtd}, visual={'sim' if is_visual else 'nao'}")
        threading.Thread(target=self.run_process, args=(qtd, city, headless), daemon=True).start()

    def run_process(self, qtd: int, city: str, headless: bool) -> None:
        try:
            scraper.run_scraper(
                target_count=qtd,
                city=city,
                headless=headless,
                log_callback=self.log_message,
            )
            self.log_message("Processo finalizado.")
        except Exception as exc:
            self.log_message(f"Erro fatal: {exc}")
        finally:
            self.after(0, lambda: self.set_running_state(False))


if __name__ == "__main__":
    app = App()
    app.mainloop()

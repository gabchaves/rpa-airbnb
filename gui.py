import json
import os
import threading
import tkinter as tk
from tkinter import messagebox, filedialog
from pathlib import Path

import customtkinter as ctk

import main as scraper

# Configurações globais de tema e aparência
ctk.set_appearance_mode("Dark")
ctk.set_default_color_theme("blue")

CONFIG_FILE = Path("config.json")
DATA_FOLDER = Path("data")

class ModernApp(ctk.CTk):
    def __init__(self):
        super().__init__()

        # --- Configuração da Janela Principal ---
        self.title("Airbnb Scraper Pro | Rápido & Eficiente")
        self.geometry("950x650")
        self.minsize(850, 550)
        
        # Grid Principal: 2 Colunas (Sidebar + Conteúdo)
        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)

        # Estado da Aplicação
        self.is_running = False
        self.output_path = "" # Vazio = usa padrao ("data")
        
        # =========================================================================
        # SIDEBAR (ESQUERDA)
        # =========================================================================
        self.sidebar = ctk.CTkFrame(self, width=280, corner_radius=0, fg_color="#1F2937") # Cinza escuro moderno
        self.sidebar.grid(row=0, column=0, sticky="nsew")
        self.sidebar.grid_rowconfigure(11, weight=1) # Empurrar footer para baixo

        # Logo / Título
        self.lbl_logo = ctk.CTkLabel(
            self.sidebar, 
            text="Airbnb\nScraper Pro", 
            font=ctk.CTkFont(family="Segoe UI", size=26, weight="bold"),
            text_color="#FF5A5F" # Vermelho Airbnb
        )
        self.lbl_logo.grid(row=0, column=0, padx=20, pady=(30, 20), sticky="w")

        # Seção: Configuração
        self.lbl_section_config = ctk.CTkLabel(
            self.sidebar, 
            text="CONFIGURAÇÃO", 
            font=ctk.CTkFont(size=12, weight="bold"),
            text_color="gray"
        )
        self.lbl_section_config.grid(row=1, column=0, padx=20, pady=(10, 5), sticky="w")

        # Entrada: Cidade
        self.entry_city = ctk.CTkEntry(
            self.sidebar, 
            placeholder_text="Cidade - UF (Ex: Manaus - AM)",
            height=40,
            border_color="#374151"
        )
        self.entry_city.grid(row=2, column=0, padx=20, pady=10, sticky="ew")

        # Entrada: Quantidade
        self.entry_qtd = ctk.CTkEntry(
            self.sidebar, 
            placeholder_text="Qtd. Perfis (Ex: 50)",
            height=40,
            border_color="#374151"
        )
        self.entry_qtd.grid(row=3, column=0, padx=20, pady=10, sticky="ew")

        # Botão: Selecionar Pasta
        self.btn_folder = ctk.CTkButton(
            self.sidebar,
            text="📂 Escolher Pasta de Saída...",
            fg_color="#374151",
            hover_color="#4B5563",
            height=35,
            command=self.select_output_folder
        )
        self.btn_folder.grid(row=4, column=0, padx=20, pady=(10, 5), sticky="ew")
        
        self.lbl_folder_status = ctk.CTkLabel(
            self.sidebar, 
            text="Pasta: Padrão (automático)", 
            font=ctk.CTkFont(size=10),
            text_color="gray"
        )
        self.lbl_folder_status.grid(row=5, column=0, padx=20, pady=(0, 10), sticky="w")


        # Switch: Headless
        self.switch_headless = ctk.CTkSwitch(
            self.sidebar, 
            text="Modo Rápido (Sem Janela)",
            font=ctk.CTkFont(size=13),
            progress_color="#00A699" # Teal Airbnb
        )
        self.switch_headless.grid(row=6, column=0, padx=20, pady=20, sticky="w")
        self.switch_headless.select()

        # Botão Principal: INICIAR (Estilo CTA)
        self.btn_start = ctk.CTkButton(
            self.sidebar,
            text="INICIAR EXTRAÇÃO",
            font=ctk.CTkFont(size=14, weight="bold"),
            fg_color="#00A699",
            hover_color="#008489",
            height=50,
            command=self.on_start_click,
            corner_radius=8
        )
        self.btn_start.grid(row=7, column=0, padx=20, pady=(10, 20), sticky="ew")

        # Status Label Simples
        self.lbl_status = ctk.CTkLabel(
            self.sidebar,
            text="Status: Aguardando comando...",
            font=ctk.CTkFont(size=12),
            text_color="gray"
        )
        self.lbl_status.grid(row=8, column=0, padx=20, pady=5, sticky="w")

        # Espaço Flexível (row 11)

        # Botão Rodapé: Abrir Pasta
        self.btn_open_folder = ctk.CTkButton(
            self.sidebar,
            text="Abrir Pasta Atual",
            fg_color="transparent",
            border_width=1,
            border_color="#374151",
            text_color="#D1D5DB",
            hover_color="#374151",
            command=self.open_current_folder
        )
        self.btn_open_folder.grid(row=12, column=0, padx=20, pady=20, sticky="ew")

        # =========================================================================
        # ÁREA DE CONTEÚDO (DIREITA)
        # =========================================================================
        self.main_frame = ctk.CTkFrame(self, corner_radius=0, fg_color="#111827") # Quase preto
        self.main_frame.grid(row=0, column=1, sticky="nsew")
        self.main_frame.grid_columnconfigure(0, weight=1)
        self.main_frame.grid_rowconfigure(1, weight=1)

        # Cabeçalho do Log
        self.header_frame = ctk.CTkFrame(self.main_frame, height=60, fg_color="transparent")
        self.header_frame.grid(row=0, column=0, sticky="ew", padx=25, pady=25)
        
        self.lbl_log_title = ctk.CTkLabel(
            self.header_frame, 
            text="Console em Tempo Real", 
            font=ctk.CTkFont(family="Segoe UI", size=20, weight="bold"),
            text_color="white"
        )
        self.lbl_log_title.pack(side="left")

        self.btn_clear_log = ctk.CTkButton(
            self.header_frame,
            text="Limpar Console",
            width=100,
            fg_color="#374151",
            hover_color="#4B5563",
            command=self.clear_logs
        )
        self.btn_clear_log.pack(side="right")

        # Área de Texto do Log (Estilo Terminal)
        self.console = ctk.CTkTextbox(
            self.main_frame,
            font=ctk.CTkFont(family="Consolas", size=13),
            text_color="#10B981", # Verde Matrix/Terminal
            fg_color="#000000",
            corner_radius=8,
            border_width=1,
            border_color="#374151"
        )
        self.console.grid(row=1, column=0, padx=25, pady=(0, 25), sticky="nsew")
        self.console.configure(state="disabled")

        # Carregar configurações salvas ao iniciar
        self.load_config()

    # --- Lógica da Aplicação ---

    def select_output_folder(self):
        folder_selected = filedialog.askdirectory()
        if folder_selected:
            self.output_path = folder_selected
            # Mostra apenas as ultimas partes do caminho pra nao quebrar layout
            display_path = "..." + folder_selected[-25:] if len(folder_selected) > 25 else folder_selected
            self.lbl_folder_status.configure(text=f"Pasta: {display_path}", text_color="#10B981")
        else:
            # Mantem o que estava ou reseta se quiser? Vamos manter.
            pass

    def log(self, message: str):
        # Apenas delega para o método thread-safe
        self.after(0, lambda: self._append_log(message))

    def _append_log(self, message: str):
        self.console.configure(state="normal")
        
        # Adiciona timestamp visual
        import time
        ts = time.strftime("[%H:%M:%S]")
        
        if not message.strip().startswith("["):
            line = f"{ts} {message}\n"
        else:
            line = f"{message}\n"
            
        self.console.insert("end", line)
        self.console.see("end") # Auto-scroll
        self.console.configure(state="disabled")

    def clear_logs(self):
        self.console.configure(state="normal")
        self.console.delete("0.0", "end")
        self.console.configure(state="disabled")

    def load_config(self):
        if CONFIG_FILE.exists():
            try:
                with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.entry_city.delete(0, "end")
                    self.entry_city.insert(0, data.get("city", ""))
                    
                    self.entry_qtd.delete(0, "end")
                    self.entry_qtd.insert(0, str(data.get("target", "10")))
                    
                    if not data.get("headless", True):
                        self.switch_headless.deselect()
                    else:
                        self.switch_headless.select()
                    
                    # Carrega ultima pasta usada? (Opcional, por seguranca melhor nao forcar pasta antiga)
            except:
                pass

    def save_config(self):
        data = {
            "city": self.entry_city.get(),
            "target": self.entry_qtd.get(),
            "headless": bool(self.switch_headless.get())
        }
        try:
            with open(CONFIG_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f)
        except:
            pass

    def open_current_folder(self):
        target = Path(self.output_path) if self.output_path else DATA_FOLDER
        target.mkdir(parents=True, exist_ok=True)
        os.startfile(target)

    def on_start_click(self):
        if self.is_running:
            return

        city = self.entry_city.get().strip()
        qtd_str = self.entry_qtd.get().strip()

        # VALIDACAO DE CIDADE
        if not city or len(city) < 3:
            messagebox.showwarning("Cidade Inválida", "Por favor, digite o nome completo da cidade (mínimo 3 letras).")
            return
        
        try:
            qty = int(qtd_str)
            if qty <= 0: raise ValueError
        except:
            messagebox.showwarning("Quantidade Inválida", "A quantidade deve ser um número inteiro positivo.")
            return
        
        headless = bool(self.switch_headless.get())
        
        # Salva para a proxima
        self.save_config()

        # Atualiza UI
        self.is_running = True
        self.btn_start.configure(state="disabled", text="EM EXECUÇÃO...", fg_color="#374151")
        self.lbl_status.configure(text="STATUS: RODANDO", text_color="#10B981") # Verde vivo
        self.clear_logs()
        
        local_msg = f"Salvando em: {self.output_path}" if self.output_path else "Salvando em: Pasta Padrão (/data)"
        self.log(f"--- INICIANDO PROCESSO PARA: {city} ({qty} perfis) ---\n{local_msg}")

        # Inicia Thread
        t = threading.Thread(target=self.run_process_thread, args=(qty, city, headless, self.output_path))
        t.daemon = True
        t.start()

    def run_process_thread(self, qty, city, headless, out_folder):
        try:
            # Chama o main.py (agora otimizado)
            # Passamos self.log que é thread-safe via after()
            scraper.run_scraper(
                target_count=qty,
                city=city,
                headless=headless,
                log_callback=self.log,
                output_folder=out_folder # Passa a pasta escolhida (ou vazia)
            )
            self.log("--- PROCESSO CONCLUÍDO COM SUCESSO ---")
            self.after(0, lambda: messagebox.showinfo("Pronto", "Raspagem concluída com sucesso!"))
            
        except Exception as e:
            self.log(f"ERRO FATAL: {str(e)}")
            self.after(0, lambda: messagebox.showerror("Erro", f"Ocorreu um erro: {e}"))
            
        finally:
            self.after(0, self.on_process_finished)

    def on_process_finished(self):
        self.is_running = False
        self.btn_start.configure(state="normal", text="INICIAR EXTRAÇÃO", fg_color="#00A699")
        self.lbl_status.configure(text="Status: Aguardando...", text_color="gray")


if __name__ == "__main__":
    app = ModernApp()
    app.mainloop()

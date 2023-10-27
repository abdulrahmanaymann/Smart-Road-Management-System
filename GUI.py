import tkinter as tk
from tkinter import ttk

from config import EXCEL_FILE
from excel_reader import read_excel_sheets


def display_data():
    df_governorates, df_vehicles, df_travels = read_excel_sheets(EXCEL_FILE)

    # Create the main window
    root = tk.Tk()
    root.title("Data Display")

    # Create notebook with tabs
    notebook = ttk.Notebook(root)
    notebook.pack(fill="both", expand=True)

    # Create tabs for each sheet
    governorates_tab = ttk.Frame(notebook)
    vehicles_tab = ttk.Frame(notebook)
    travels_tab = ttk.Frame(notebook)

    notebook.add(governorates_tab, text="Governorates")
    notebook.add(vehicles_tab, text="Vehicles")
    notebook.add(travels_tab, text="Travels")

    # Create text widgets to display data
    governorates_text = tk.Text(governorates_tab)
    governorates_text.insert(tk.END, df_governorates.to_string())
    governorates_text.pack(fill="both", expand=True)

    vehicles_text = tk.Text(vehicles_tab)
    vehicles_text.insert(tk.END, df_vehicles.to_string())
    vehicles_text.pack(fill="both", expand=True)

    travels_text = tk.Text(travels_tab)
    travels_text.insert(tk.END, df_travels.to_string())
    travels_text.pack(fill="both", expand=True)

    root.mainloop()

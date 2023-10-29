import tkinter as tk
from tkinter import ttk
from tkinter import messagebox
from tkinter import simpledialog

from data_handler import *
from config import EXCEL_FILE
from excel_reader import read_excel_sheets


def add_travel_record(df_travels):
    id = simpledialog.askstring("New Travel Record", "Enter ID (e.g., 789_Car):")
    start_gate = simpledialog.askstring("New Travel Record", "Enter Start Gate:")
    end_gate = simpledialog.askstring("New Travel Record", "Enter End Gate:")
    distance = simpledialog.askfloat("New Travel Record", "Enter Distance (KM):")

    if id and start_gate and end_gate and distance is not None:
        # Add the new travel record to the DataFrame
        df_travels = process_new_travel_data(
            df_travels, id, start_gate, end_gate, distance
        )
        messagebox.showinfo("Success", "Travel record added successfully.")
    else:
        messagebox.showerror("Error", "Please fill in all fields.")

    return df_travels


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

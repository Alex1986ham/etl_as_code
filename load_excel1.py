import pandas as pd

file_location = "I:/obiatotto/_Übergreifende Themen und Projekte/OPIS/Schnittstelle/Schnittstellen_Templates_endgueltig/VS_FELIS/Abwertung.xlsx"

df = pd.DataFrame(pd.read_excel(io = file_location, header = None, names = column_names))


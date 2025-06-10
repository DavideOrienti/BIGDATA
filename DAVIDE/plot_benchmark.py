import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv('./data/benchmark_results.csv')

pivot = df.pivot(index='input_size', columns='job', values='time_sec')
print("\n📊 Tabella Tempi di Esecuzione:")
print(pivot)

pivot.plot(marker='o', title="Tempo di Esecuzione vs Dimensione Input")
plt.xlabel("Numero di Righe")
plt.ylabel("Tempo (secondi)")
plt.grid(True)
plt.tight_layout()
plt.savefig("./data/benchmark_plot.png")
plt.show()

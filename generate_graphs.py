import matplotlib
matplotlib.use('Agg')  # IMPORTANT: use non-interactive backend BEFORE pyplot import
import matplotlib.pyplot as plt
import os

def main():
    output_dir = os.path.join('static', 'graphs')
    os.makedirs(output_dir, exist_ok=True)

    # Example graph
    plt.plot([1, 2, 3], [3, 2, 5])
    plt.title("Demo Graph")
    plt.savefig(os.path.join(output_dir, 'example.png'))
    plt.close()

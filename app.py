from flask import Flask, send_from_directory, render_template, redirect, url_for
from flask import jsonify
import os
import generate_db
import step1_create_db_from_tickers_list
import step2_enrich_tickers_with_yfinance
import step3_create_candidate_db
import step4_process_candidates_db
from step5_sectors_performances import get_sector_performance_table
import step6_create_candidates_db_price_history
import step7_plot_candidates

app = Flask(__name__)

GRAPH_FOLDER = os.path.join('static', 'graphs')

@app.route('/')
def index():
    images = os.listdir(GRAPH_FOLDER)
    images = [f for f in images if f.endswith('.png')]
    return render_template('index.html', images=images)

@app.route('/generate-db')
def generate_db_route():
    generate_db.main()  # Assuming your DB script has a main() function
    return redirect(url_for('index'))

@app.route('/enrich-tickers')
def enrich_tickers_route():
    step2_enrich_tickers_with_yfinance.main()
    return redirect(url_for('index'))

@app.route('/create-candidate-db')
def create_candidate_db_route():
    step3_create_candidate_db.main()
    return redirect(url_for('index'))

@app.route('/process-candidates-db')
def process_candidates_db_route():
    step4_process_candidates_db.main()
    return redirect(url_for('index'))

@app.route('/sectors-performances')
def sectors_performances():
    return render_template('loading.html', next_url=url_for('sectors_performances_data'))

@app.route('/sectors-performances/data')
def sectors_performances_data():
    _, table_data = get_sector_performance_table()
    return render_template('sectors.html', table_data=table_data)

@app.route('/create-candidates-db-price-history')
def create_candidates_db_price_history_route():
    step6_create_candidates_db_price_history.main()
    return redirect(url_for('index'))


@app.route('/generate-graphs')
def generate_graphs_loading():
    return render_template('loading.html', next_url=url_for('generate_graphs_process'))

@app.route('/generate-graphs/process')
def generate_graphs_process():
    step7_plot_candidates.main()  # Assuming your graph script has a main() function
    return redirect(url_for('index'))

@app.route('/graphs/<filename>')
def get_graph(filename):
    return send_from_directory(GRAPH_FOLDER, filename)



@app.route('/generate-graphs/live')
def generate_graphs_live():
    return render_template('generate_graphs_ajax.html')

@app.route('/generate-graphs/api', methods=['POST'])
def api_generate_graphs():
    step7_plot_candidates.main()  # Assuming your graph script has a main() function
    return jsonify({"status": "done"})






if __name__ == '__main__':
    app.run(debug=True, port=5052)

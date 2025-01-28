# Redistribution of Wealth on Blockchain: A Thesis
This repository contains the code for the thesis "Redistribution of Wealth on Blockchain".\
The research explores how different redistribution strategies, particularly block fee redistribution, affect wealth inequality within blockchain systems. Key metrics like the Gini coefficient are analyzed to assess inequality under varying redistribution models.

## 📜 Overview
The project investigates:

- Redistribution of block fees, block rewards, taxes on transactions among eligible addresses on Bitcoin.
- The impact of various redistribution strategies on wealth inequality.
- The use of Gini and Nakamoto coefficients as a measure of inequality in wealth distribution.

## Key Features
Simulations: Implementation of blockchain transactions with redistribution mechanisms.\
Analytics: Gini and Nakamoto coefficients computation to measure the inequality of wealth distribution.\
Visualization: Plots and charts to illustrate the effects of different redistribution rates.\
Flexibility: Configurable parameters, such as redistribution thresholds and algorithms.

## 🚀 Getting Started
Follow these steps to set up the project locally and begin your exploration:

### Prerequisites
Python 3.8+\
Required libraries (listed in requirements.txt):
````
base58
tqdm
pyyaml
pandas
tables
numpy_minmax
matplotlib
````
### Installation
Clone the repository:
````
bash
git clone https://github.com/CarloGiralda/Thesis.git
````
Navigate to the project directory:
````
bash
cd Thesis
````
Install dependencies:
````
bash
pip install -r requirements.txt
````

## 🧪 Usage

### Simulations
Edit the configuration file (config.yaml) to adjust parameters like the redistribution rate, threshold, and transaction details.\
Run the simulation (e.g., simple redistribution):
````
bash
python redistribution.py
````
### Visualizations
After running simulations, plots are automatically generated to visualize Gini coefficients and wealth distribution.

## 📂 Repository Structure
````
├── data_dungeon/                     # Source code for simulations
│   ├── database/                     # Source code for database interaction
│   ├── file_parsing/                 # Source code for extraction of blocks from Bitcoin folder
│   ├── only_redistribution_space/    # Source code for simulations on redistributions only
│   ├── redistribution_space/         # Source code for simulations on balances and redistributions
│   ├── tests/                        # Files for testing *_paradise.py files
│   ├── utxo_hub/                     # Source code for extraction of UTXOs from Bitcoin folder and conversion to SQLite3 database
│   ├── weath_metrics/                # Implementation of wealth metrics
├── CITATION.cff                      # File for CITATION
├── README.md                         # Project documentation
├── requirements.txt                  # Python dependencies
````

## 📚 Research Insights

### Key Findings
- Redistribution strategies directly impact wealth inequality.
- Equal distribution is a possible solution, as demonstrated in the simulations.
- Thresholds for redistribution play a critical role in influencing the outcomes.
- Inversely proportional redistribution showed promising results but requires careful calibration.
### Future Directions
- Exploring more advanced redistribution algorithms, such as weighted or utility-based models.
- Expanding the simulation to incorporate real-world blockchain data.
- Integrating additional inequality metrics.

## 🤝 Contributing
Contributions to this project are welcome! Please fork the repository, make your changes, and submit a pull request.

## 📄 License

## 🛠️ Acknowledgments
Academic advisor: [Andrea Vitaletti]\
Inspiration from existing research on blockchain economics and wealth redistribution.\
This project incorporates code and concepts from the following repositories:
1. **[blockchain-parser](https://github.com/ragestack/blockchain-parser)**
   - Description: Used for implementing the block extraction.
   - Author(s): ragestack
2. **[python-bitcoin-blockchain-parser](https://github.com/alecalve/python-bitcoin-blockchain-parser)**
   - Description: Used for implementing the block extraction.
   - Author(s): alecalve

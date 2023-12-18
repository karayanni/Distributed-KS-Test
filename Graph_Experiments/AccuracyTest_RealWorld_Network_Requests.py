import matplotlib.pyplot as plt

from utils import *
from sklearn.datasets import fetch_kddcup99
import pandas as pd
import numpy as np

ds = fetch_kddcup99(as_frame=True)
df = pd.concat([ds.data, ds.target], axis=1)


def run_test(df_param):
    reference_data = df_param['src_bytes'].head(50000).tolist()
    test_data_all = df_param['src_bytes'][50000:].tolist()
    chunk_size = 50000
    exact_ks = []
    digest_ks = []
    for i in range(0, len(test_data_all), chunk_size):
        test_data = test_data_all[i:i + chunk_size]

        original_ks = get_original_ks(reference_data, test_data)
        print(f'original ks_score: {original_ks}')
        exact_ks.append(original_ks)

        dig_val = get_digest_ks(reference_data, test_data)
        print(f'Digest KS score: {dig_val}')
        digest_ks.append(dig_val)

        with open("experiment_results/AccuracyTest_RealWorld_Network_Requests", 'a') as file:
            file.write(f'Iteration:{i} - original ks_score: {original_ks}' + '\n')
            file.write(f'Iteration:{i} - T_Digest ks_score: {dig_val}' + '\n')
    with open("experiment_results/AccuracyTest_RealWorld_Network_Requests", 'a') as file:
        file.write(f'FINAL RESULTS' + '\n')
        file.write(f'Original ks_scores: {exact_ks}' + '\n')
        file.write(f'Digest ks_scores: {digest_ks}' + '\n')


def plot_exact_KS2(ground_t_l, figure_name="figure"):
    batches = range(1, len(ground_t_l) + 1)
    # Plot ground truth
    plt.plot(batches, ground_t_l, label='Exact KS Statistic', linestyle=(0, (1, 10)), linewidth=2, marker='o',
             markersize=15)

    # Add labels, title, and legend
    plt.xlabel('Batch Number', fontsize=16)
    plt.ylabel('Exact  KS  Statistic', fontsize=16)
    # plt.legend(fontsize=16)
    plt.xticks(batches, fontsize=16)
    plt.yticks(fontsize=16)

    # Show the plot
    plt.savefig(f'experiment_results/figures/{figure_name}.pdf', format='pdf', bbox_inches='tight')
    plt.show()
    plt.close()


def plot_errors_line(ground_t_l, digest_l, figure_name="figure"):
    # Calculate the Error
    differences_digest = [est2 - true for est2, true in zip(digest_l, ground_t_l)]

    batches = range(1, len(ground_t_l) + 1)
    plt.scatter(batches, differences_digest, color='red', label='T-Digest', marker='x', s=200)

    plt.axhline(0, color='grey', lw=2, linestyle='--')

    # Label the axes
    plt.xlabel('Batch  Number', fontsize=16)
    plt.ylabel('T-Digest  Error', fontsize=16)

    # plt.title('Comparison of Estimation Methods')
    # Add a legend
    # plt.legend(loc='lower right', fontsize=16)

    plt.xticks(batches, fontsize=16)
    plt.yticks(fontsize=16)

    # Show the plot
    plt.savefig(f'experiment_results/figures/{figure_name}.pdf', format='pdf', bbox_inches='tight')

    plt.show()
    plt.close()


if __name__ == '__main__':
    # run_test(df)
    original_ks_scores = [0.45216, 0.49708, 0.7229, 0.7229, 0.7229, 0.59802, 0.87088, 0.69458, 0.6416049685377433]
    digest_ks_scores = [0.45137292227668946, 0.4964921292424579, 0.7653199999999986, 0.72334, 0.72338, 0.6404639999999986,
                0.870923216374269, 0.6711194584740576, 0.6413784409868566]

    plot_exact_KS2(original_ks_scores, "AccuracyTest_RealWorld_Network_Requests_Exact_KS_Graph")
    plot_errors_line(original_ks_scores, digest_ks_scores, "AccuracyTest_RealWorld_Network_Requests_TDigest_Errors_Graph")

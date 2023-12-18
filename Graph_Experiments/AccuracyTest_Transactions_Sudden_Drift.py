import pandas as pd
from utils import *
import random
import matplotlib.pyplot as plt
import seaborn as sns

df_drift = pd.read_csv('../data/fraud/PS_20174392719_1491204439457_log.csv')
df_drift = df_drift[['amount']]

df_train = pd.read_csv('../data/fraud/fraudTrain.csv')
df_train['amount'] = df_train['amt']

NUM_SUPPORT_ITERATIONS = 15
new_dist_percentages = [0, 5, 10, 20, 50]

TAKE_ABS_ERRORS = True


def run_eval_ks(train_ds_list, test_ds_list):
    chunk_size = 500000
    ref_data = train_ds_list[:chunk_size]

    for new_percentage in new_dist_percentages:
        average_ground_truth_ks = 0
        drift_errors = []
        # 5% from new distribution
        for supp in range(NUM_SUPPORT_ITERATIONS):
            test_data = random.sample(train_ds_list[chunk_size:], int(chunk_size * (0.01*(100 - new_percentage))))
            test_data = test_data + random.sample(test_ds_list, chunk_size * new_percentage)
            original_ks = get_original_ks(ref_data, test_data)
            average_ground_truth_ks = average_ground_truth_ks + (original_ks / NUM_SUPPORT_ITERATIONS)
            print(f'{new_percentage}% supp:{supp} from new sample has KS score: {original_ks}')

            dig_val = get_digest_ks(ref_data, test_data)
            drift_errors.append(dig_val - original_ks)
            print(f'{new_percentage}% supp:{supp} from new sample has KS score: {dig_val}')
            with open('experiment_results/AccuracyTest_Transactions_Sudden_Drift.txt', 'a') as file:
                file.write(
                    f'{new_percentage}% from new sample - For Batch {supp}, the Digest error is: {drift_errors[supp]} --- TRUE_KS:{original_ks} DIGEST_KS:{dig_val}\n')

        with open('experiment_results/AccuracyTest_Transactions_Sudden_Drift.txt', 'a') as file:
            file.write(f'ALL ERRORS FOR {new_percentage}%: {drift_errors} \n')
            file.write(f'Average True KS for {new_percentage}%: {average_ground_truth_ks} \n')


def plot_graph(figure_name):
    errors_0 = [0.0006660189837035422, -7.821681864264574e-06, 0.000566425466035532, 0.00045727131493215976, 0.0006865379709670629, 0.0017801650387723024, 0.0003429368294116074, 4.684972463381027e-05, -2.5469495322993745e-05, 0.00031087708946402406, 0.0004344837545198193, 0.00037559606970073885, 8.834969183684101e-05, -2.3265952650862454e-05, 0.0006639978070445839]
    errors_5 = [-0.0001575466669211517, -0.0001483060892622512, -0.00034781336586331424, 0.00012872941010043382,
                0.0003441403578011426, 0.0002696512668117282, -0.0005321529030635555, -0.00016181425497353852,
                -0.0001494345687017909, -0.0007137416391827939, 0.0001969269167639981, -2.2123670066327705e-05,
                -0.00037879009714972267, -1.411146866053814e-06, -0.0003275765153320301]
    errors_10 = [-0.0001860257107886426, -6.474562092505243e-05, -0.00023031568237341649, -0.0002225534841844845,
                 -5.824152056375931e-05, 2.0084776977125074e-05, -0.0001405114169703292, 1.7329881211280984e-05,
                 -0.00023561194007823416, -0.00020767925236041263, 7.358523505704428e-05, -0.00019225891923528582,
                 -0.00021193978960129534, 3.379316402943311e-06, -2.938845563280057e-06]
    errors_20 = [-5.702201446169952e-05, 0.00019230856587310052, -0.00015594549965100568, 0.0005867877961069634,
                 -8.762808208512274e-05, -2.3066252470005377e-06, 0.00034783833333332237, -0.00028171565269408116,
                 -0.00012255325426610564, 0.000341998839743729, -4.183382357300647e-05, 9.425684901359643e-05,
                 0.00047532433518956974, 0.00032383327122195715, 0.0004056752431912669]
    errors_50 = [0.0002924568020310492, -0.0001549913391385438, 0.0009696331074813824, 0.00022331268267250515,
                 0.00014034710329935418, -4.102686207624329e-05, -9.232476535853129e-05, 4.667161579235968e-05,
                 -9.431223110756282e-06, -7.979352203540957e-05, 0.0004442777830921951, 0.00017203710646451764,
                 -0.00027536664490385165, -0.00017028036094340893, 7.785266528770318e-05]

    data = [errors_0, errors_5, errors_10, errors_20, errors_50]
    labels = ['0%', '5%', '10%', '20%', '50%']  # Labels for each boxplot

    if TAKE_ABS_ERRORS:
        for errors_list in data:
            for i in range(len(errors_list)):
                errors_list[i] = abs(errors_list[i])

    sns.boxplot(data=data, color="navajowhite", showfliers=False)

    plt.xticks(range(len(labels)), labels, fontsize=20)  # Set the x-axis labels
    plt.xlabel('Shifted  Samples (%)', fontsize=20)  # Set the x-axis label
    plt.ylabel('KS  Error', fontsize=20)  # Set the y-axis label
    plt.yticks(fontsize=20)
    # Display the plot
    plt.savefig(f'experiment_results/figures/{figure_name}.pdf', format='pdf', bbox_inches='tight')
    plt.show(bbox_inches='tight')

#
# def run_eval_ks_0_drift(train_ds_list, test_ds_list):
#     chunk_size = 300000
#     ref_data = train_ds_list[:chunk_size]
#     digest_scores = []
#     original_scores = []
#     for new_percentage in [0]:
#         drift_errors = []
#         # 5% from new distribution
#         for supp in range(NUM_SUPPORT_ITERATIONS):
#             test_data = random.sample(train_ds_list[chunk_size:], int(chunk_size * (0.01 * (100 - new_percentage))))
#             original_ks = get_original_ks(ref_data, test_data)
#             print(f'{new_percentage}% from new sample has KS score: {original_ks}')
#             original_scores.append(original_ks)
#             dig_val = get_digest_ks(ref_data, test_data)
#             print(f'{new_percentage}% from new sample has DIGEST score: {dig_val}')
#             drift_errors.append(dig_val - original_ks)
#             digest_scores.append(dig_val)
#             with open('experiment_results/AccuracyTest_Transactions_Sudden_Drift.txt', 'a') as file:
#                 file.write(
#                     f'{new_percentage}% from new sample - For Batch {supp}, the Digest error is: {drift_errors[supp]} --- TRUE_KS:{original_ks} DIGEST_KS:{dig_val}\n')
#
#         with open('experiment_results/AccuracyTest_Transactions_Sudden_Drift.txt', 'a') as file:
#             file.write(f'ALL ERRORS FOR {new_percentage}%: {drift_errors} \n')
#             file.write(f'ALL Original KS score {original_scores} \n')
#             file.write(f'ALL DIGEST KS score {digest_scores} \n')


if __name__ == '__main__':
    # train_list = df_train["amount"].tolist()
    # test_list = df_drift["amount"].tolist()
    # run_eval_ks(train_list, test_list)
    #
    plot_graph("AccuracyTest_Transactions_Sudden_Drift")

import pandas as pd
import numpy as np
from utils import *
import random
import seaborn as sns


file_path = '../data/fraud/real_world_creditcard_dep.csv'
df = pd.read_csv(file_path)
df = df[["Amount"]]
TAKE_ABS_ERRORS = True


def run_linear_drift_test():
    NUM_SUPPORT_ITERATIONS = 15

    average_ground_truths = [0]*5
    digest_errors_per_batch_index = {}

    # init each with an empty list to be filled by the errors.
    for i in range(5):
        digest_errors_per_batch_index[i] = []

    for supp in range(NUM_SUPPORT_ITERATIONS):
        new_data = np.random.choice(df['Amount'], size=600000-284807).tolist()

        real_life_items = df["Amount"].tolist()
        dataset = real_life_items + new_data
        random.shuffle(dataset)

        reference_data = dataset[:100000]

        chunk_size = 100000
        test_data_all = dataset[100000:]

        for index, chunk in enumerate(range(0, len(test_data_all), chunk_size)):
            for i in range(chunk, min(chunk_size+chunk, len(test_data_all))):
                test_data_all[i] = test_data_all[i] + (index*(index-0.5)*np.random.normal(3.5, 1))

        for index, i in enumerate(range(0, len(test_data_all), chunk_size)):
            test_data = test_data_all[i:i + chunk_size]
            original_ks = get_original_ks(reference_data, test_data)
            digest_ks = get_digest_ks(reference_data, test_data)

            digest_errors_per_batch_index[index].append(digest_ks - original_ks)
            print(f'error in supp: {supp}, batch: {index} is: {digest_ks - original_ks}, where, digest:{digest_ks} and original:{original_ks}')
            average_ground_truths[index] = average_ground_truths[index] + (original_ks/NUM_SUPPORT_ITERATIONS)
        print("----------------------------------------------------")

    for i in range(5):
        print(f'For Batch {i}, the errors are: \n')
        print(digest_errors_per_batch_index[i])
        print("\n")

    for i in range(5):
        with open('experiment_results/AccuracyTest_RealWorld_Linear_Drift.txt', 'a') as file:
            file.write(f'For Batch {i}, the errors are: {digest_errors_per_batch_index[i]} --- Average Error for batch: {average_ground_truths[i]}\n')


def plot_linear_drift(figure_name):
    errors__batch_1 = [0.003249682561656899, 0.0030789682207203475, 0.00276602009533181, 0.004828636919028153, 0.0021760575010173443, 0.005460144879716818, 0.002562906348910881, 0.004487768205029619, 0.004683567335243509, 0.0040580832735104, 0.004453990142660114, 0.0033651054980726285, 0.004727232401839503, 0.0034489076938657814, 0.003694247600070715]
    min_max_KS_batch_1 = "[0,0.01]"

    errors__batch_2 = [-0.0013699315104568899, -0.001317029981060397, -0.0013721361066276194, -0.001847338921050462, -0.0019692739378978363, -0.00039716322490074285, -0.0019165374430804571, -0.0018707325981354161, -0.0020297506061343895, -0.0019642341722532763, -0.0019444678552030803, -0.0020075370901467826, -0.0018721438835014037, -0.0018927545671673651, -0.0017063429737546598]
    min_max_KS_batch_2 = "[0.14,0.16]"

    errors__batch_3 = [-0.0023988829678989743, -0.0008198582057564185, -0.0007751212508150807, -0.0006269024487062791, -0.00020438885854195954, -0.0002200599305191564, -0.0045399999999999885, -0.0023406331321991214, -0.00037807016227897616, -0.0011789406220556464, -0.00031607913753012307, -0.0008667638558197965, -0.0007295805432878111, -0.0002544369771482202, -0.00039881678869058135]
    min_max_KS_batch_3 = "[0.28,0.32]"

    errors__batch_4 = [-0.0021749041620652276, -0.00040603327310023696, -0.00047167404282016934, -0.0006705135940754947, -0.0001915241872550988, -0.00036031430029670464, -0.00013124500682054618, -0.0002845372674548652, -0.001458900874053315, -0.0014814420263469796, -0.0011616858891479453, -0.00181564913599086, -0.0007226534536731166, -0.0003271887580603705, -0.0006142472989528747]
    min_max_KS_batch_4 = "[0.42,0.48]"

    errors__batch_5 = [-0.0008771268920902964, -0.0006735627412403078, -0.0022142592388094684, -0.0010254209571035622, -0.0007383348784743005, -0.0012460295833953694, -0.0014636511486236792, -0.0009453256506544161, -0.0013417174164480627, -0.001141430105007557, -0.0021081535201934454, -0.001042441966471186, -0.0005177951545656256, -0.0004400379940112664, -0.00043403886996629115]
    min_max_KS_batch_5 = "[0.51,0.59]"

    data = [errors__batch_1, errors__batch_2, errors__batch_3, errors__batch_4, errors__batch_5]
    # labels = ['0.003', '0.151', '0.295', '0.450', '0.550']  # Labels for each boxplot
    labels = [min_max_KS_batch_1, min_max_KS_batch_2, min_max_KS_batch_3, min_max_KS_batch_4, min_max_KS_batch_5]

    if TAKE_ABS_ERRORS:
        for errors_list in data:
            for i in range(len(errors_list)):
                errors_list[i] = abs(errors_list[i])

    sns.boxplot(data=data, color="navajowhite", showfliers=False)

    plt.xticks(range(len(labels)), labels, fontsize=14)  # Set the x-axis labels
    plt.xlabel('KS  Value Range', fontsize=20)  # Set the x-axis label
    plt.ylabel('KS  Errors', fontsize=20)  # Set the y-axis label
    plt.yticks(fontsize=16)
    # Display the plot
    plt.savefig(f'experiment_results/figures/{figure_name}.pdf', format='pdf', bbox_inches='tight')
    plt.show(bbox_inches='tight')


if __name__ == '__main__':
    run_linear_drift_test()
    # plot_linear_drift('AccuracyTest_RealWorld_Linear_Drift_Graph')

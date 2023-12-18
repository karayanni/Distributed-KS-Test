import time
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from edge_client.client import EdgeClient

file_path = '../data/fraud/real_world_creditcard_dep.csv'
df = pd.read_csv(file_path)
df = df[["Amount"]]
dataset = df["Amount"].tolist()


def client_eval():
    SUPPORT_ITERATIONS = 10

    for supp in range(SUPPORT_ITERATIONS):
        for client_batch_size in range(1000, 100000, 5000):
            batch_values = np.random.choice(dataset, size=client_batch_size).tolist()

            def histo_eval(hist_batch_values):
                start_time = time.time()
                client_handler = EdgeClient()
                max_val = max(dataset)
                min_val = min(dataset)

                def get_create_histogram():
                    num_bins = 1000
                    return [0] * (num_bins + 2)

                def update_histogram(histogram, sample):
                    num_bins = 1000

                    step = (max_val - min_val) / num_bins

                    def get_index(val):
                        if val < min_val:
                            bin_index = 0
                        elif val > max_val:
                            bin_index = num_bins + 1
                        else:
                            # Subtract min_val to start from 0 and then divide by step to find the appropriate bin.
                            # Add 1 to skip the bin reserved for <min_val
                            bin_index = 1 + int((val - min_val) / step)

                            # Handle edge case where value equals max_val. It should be placed in the max-step --> max bin.
                            if bin_index == num_bins + 1:
                                bin_index -= 1

                        return bin_index

                    histogram[get_index(sample)] += 1

                hist = get_create_histogram()

                for val in hist_batch_values:
                    update_histogram(hist, val)

                client_handler.send_any_object(hist)
                total_time = time.time() - start_time
                print(f'Client Histogram with batch size: {len(hist_batch_values)} finished in: {total_time}')
                client_handler.clear_queue_from_all_messages()
                return total_time

            def t_digest_eval(digest_batch_values):
                start_time = time.time()
                client_handler = EdgeClient()

                client_handler.update_digest_with_vals(digest_batch_values)

                client_handler.send_t_digest()

                total_time = time.time() - start_time
                print(f'Client T_Digest with batch size: {len(digest_batch_values)} finished in: {total_time}')
                client_handler.clear_queue_from_all_messages()
                return total_time

            # this is not finishing :) will probably need to simulate it manually...
            def stream_client_eval(stream_batch_values):
                start_time = time.time()
                client_handler = EdgeClient()
                for val in stream_batch_values:
                    client_handler.send_any_object(val)

                total_time = time.time() - start_time
                print(f'Client Stream with batch size: {len(stream_batch_values)} finished in: {total_time}')
                client_handler.clear_queue_from_all_messages()
                return total_time

            def streaming_client_eval_with_catch_sending(stream_batch_values):
                start_time = time.time()
                client_handler = EdgeClient()
                curr_batch = []
                for val in stream_batch_values:
                    curr_batch.append(val)
                    if len(curr_batch) == 1000:
                        client_handler.send_any_object(curr_batch)
                        curr_batch = []

                if len(curr_batch) != 0:
                    print("Not divisible by 1000???")
                    client_handler.send_any_object(curr_batch)

                total_time = time.time() - start_time
                print(f'Client Stream with batch size: {len(stream_batch_values)} finished in: {total_time}')
                client_handler.clear_queue_from_all_messages()
                return total_time

            hist_time = histo_eval(batch_values)
            t_digest_time = t_digest_eval(batch_values)
            batch_streaming_digest = streaming_client_eval_with_catch_sending(batch_values)
            with open("aggregation_final_times_3_methods", 'a') as file:
                file.write(f'{client_batch_size} items - HIST TIME: {hist_time} ' + '\n')
                file.write(f'{client_batch_size} items - dist_t_digest TIME: {t_digest_time} ' + '\n')
                file.write(f'{client_batch_size} items - streaming batch digest TIME: {batch_streaming_digest} ' + '\n')

        with open("aggregation_final_times_3_methods", 'a') as file:
            file.write(f'----NEW SUPPORT--NEW SUPPORT--NEW SUPPORT--NEW SUPPORT----' + '\n')

    print("client eval finished")


def plot_client_runtimes(t_digest_times: dict, streaming_times: dict, figure_name="figure"):
    keys2, values2 = zip(*t_digest_times.items())
    keys3, values3 = zip(*streaming_times.items())

    # Plot ground truth
    plt.plot(keys2, values2, label='T-Digest-Merge')
    plt.plot(keys3, values3, label='T-Digest-Stream')

    # Add labels, title, and legend
    plt.xlabel('d', fontsize=20)
    plt.ylabel('Runtime (Seconds)', fontsize=20)
    plt.legend(fontsize=16)
    plt.yticks(fontsize=16)
    plt.xticks(fontsize=16)

    # Show the plot
    plt.savefig(f'experiment_results/figures/{figure_name}.pdf', format='pdf', bbox_inches='tight')
    plt.show()
    plt.close()


if __name__ == '__main__':
    client_Hist_runtimes_supported = {1000: 0.3093174934387207, 6000: 0.31273660659790037, 11000: 0.3203922748565674,
                                      16000: 0.3194575309753418, 21000: 0.32211325168609617, 26000: 0.3216524362564087,
                                      31000: 0.3316672325134277, 36000: 0.3289741039276123, 41000: 0.3352306365966797,
                                      46000: 0.33513972759246824, 51000: 0.34089787006378175,
                                      56000: 0.34087769985198973,
                                      61000: 0.3478144407272339, 66000: 0.34857792854309083, 71000: 0.34890482425689695,
                                      76000: 0.353889536857605, 81000: 0.35634241104125974, 86000: 0.36712894439697263,
                                      91000: 0.37523751258850097, 96000: 0.3763295650482178}
    client_digest_runtimes_supported = {1000: 0.33727877140045165, 6000: 0.463724684715271, 11000: 0.6017499208450318,
                                        16000: 0.7242050409317017, 21000: 0.8637821674346924, 26000: 0.9863433837890625,
                                        31000: 1.145041036605835, 36000: 1.2625914096832276, 41000: 1.4032079219818114,
                                        46000: 1.410219669342041, 51000: 1.5187960386276245, 56000: 1.645762062072754,
                                        61000: 1.844553828239441, 66000: 1.8796077728271485, 71000: 2.024828839302063,
                                        76000: 2.158160591125488, 81000: 2.264967942237854, 86000: 2.680700087547302,
                                        91000: 2.8757016181945803, 96000: 2.9846495628356933}
    client_stream_runtimes_supported = {1000: 0.2990242004394531, 6000: 1.7027153015136719, 11000: 3.403655171394348,
                                        16000: 4.806126284599304, 21000: 6.309966731071472, 26000: 7.9149717330932615,
                                        31000: 9.275182375907899, 36000: 10.618561053276062, 41000: 12.221786499023437,
                                        46000: 13.83541100025177, 51000: 15.330073380470276, 56000: 16.641761422157287,
                                        61000: 18.261210894584655, 66000: 19.833101797103883, 71000: 21.29469386100769,
                                        76000: 22.919548845291138, 81000: 24.15674614906311, 86000: 25.755253291130065,
                                        91000: 27.391878056526185, 96000: 28.873185324668885}
    plot_client_runtimes(client_digest_runtimes_supported, client_stream_runtimes_supported, "Runtimes_client")

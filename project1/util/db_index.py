import collections
import pickle

def create_index(db):

    end = 0
    start = 0
    gene_name = ''

    index = dict()

    with open(db, 'r') as f:
        for line_num, line in enumerate(f,1):
            if line.startswith(">") or not line.strip():
                gene_name = line.strip()[1:]
                start = 0
                end = 0
                continue
            else:
                line = line.strip()
                start = end + 1
                end += len(line)

                if gene_name in index.keys():
                    if line_num not in index[gene_name]:
                        index[gene_name][line_num] = (start,end)

                else:
                    index[gene_name]= { line_num : (start,end)}



                # print(gene_name, "start:"+ str(start), "end:"+str(end), line_num, len(line), line)


        # print(index)
        pickle.dump(index, open(db + '.fai', "wb"))

        return 0

import csv

def main():


    with open("gene_info") as f:
        reader = csv.reader(f, delimiter="\t")
        
        tax_index = 0
        id_index = 1
        sym_index = 2
        #bar delimited
        syns_index = 4
        


        header = True

        entries = 0

        for line in reader:
            #skip header
            if header:
                header = False
                continue
            tax_id = line[tax_index]
            gene_id = line[id_index]
            symbol = line[sym_index]

            #doesn't look like any of the symbols are blank, but just in case should skip record if it is
            if symbol == "-":
                continue

            synonyms = line[syns_index]
            if synonyms == "-":
                synonyms = []
            else:
                synonyms = synonyms.split("|")
            
            names = [symbol, *synonyms]


            #table format
            # name (sym or syn), id, tax

            for name in names:
                entries += 1


        print(entries)



        
        
if __name__ == "__main__":
    main()
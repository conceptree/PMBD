import csv

# Nome do arquivo de entrada e saída
input_file = './assets/ghcnd-stations.txt'
output_file = './assets/ghcnd_stations.csv'

# Abrir arquivo de saída
with open(output_file, mode='w', newline='') as csv_file:
    csv_writer = csv.writer(csv_file)

    # Escrever cabeçalho
    header = ['station_id', 'latitude', 'longitude', 'elevation', 'state', 'name', 'gsn_flag', 'hcn_crn_flag', 'wmo_id']
    csv_writer.writerow(header)

    # Ler arquivo de entrada e processar linha por linha
    with open(input_file, mode='r') as txt_file:
        for line in txt_file:
            # Extrair dados baseado nas posições fixas das colunas
            station_id = line[0:11].strip()
            latitude = line[12:20].strip()
            longitude = line[21:30].strip()
            elevation = line[31:37].strip()
            state = line[38:40].strip()
            name = line[41:71].strip()
            gsn_flag = line[72:75].strip()
            hcn_crn_flag = line[76:79].strip()
            wmo_id = line[80:85].strip()

            # Escrever linha no arquivo CSV
            csv_writer.writerow([station_id, latitude, longitude, elevation, state, name, gsn_flag, hcn_crn_flag, wmo_id])

print(f"Arquivo CSV '{output_file}' criado com sucesso.")

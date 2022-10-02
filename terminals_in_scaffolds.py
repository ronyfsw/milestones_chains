from modules.config import *
run_dir_path = os.path.join(os.getcwd(), 'run_dir_ems')
scaffolds_path1 = os.path.join(run_dir_path, 'scaffolds')
scaffolds_files = os.listdir(scaffolds_path1)
scaffolds_count = len(scaffolds_files)
nodes_decoder = np.load(os.path.join(run_dir_path, 'nodes_decoder.npy'), allow_pickle=True)[()]

scaffolds_sizes = []
missing_terminals = ['MI20-EMS-N2-108960', 'MI20-EMS-N2-300705', 'MI20-EMS-N2-507954', 'MI20-EMS-N2-526578', 'MI20-EMS-N2-60820', 'MI20-EMS-N2-62830', 'MI20-EMS-N2-107008', 'MI20-EMS-N2-63120', 'MI20-EMS-N2-61850', 'MI20-EMS-N2-94630', 'MI20-EMS-N2-61000', 'MI20-EMS-N2-108640', 'MI20-EMS-N2-62130', 'MI20-EMS-N2-524960', 'MI20-EMS-N2-603562', 'MI20-EMS-N2-111724', 'MI20-EMS-N2-61450', 'MI20-EMS-N2-120728', 'MI20-EMS-N2-106030', 'MI20-EMS-N2-510030', 'MI20-EMS-N2-61790', 'MI20-EMS-N2-62570', 'MI20-EMS-N2-122148', 'MI20-EMS-N2-60520', 'MI20-EMS-N2-95646', 'MI20-EMS-N2-116960', 'MI20-EMS-N2-92037', 'MI20-EMS-N2-63030', 'MI20-EMS-N2-62910', 'MI20-EMS-N2-523510', 'MI20-EMS-N2-525230', 'MI20-EMS-N2-300192', 'MI20-EMS-N2-115332', 'MI20-EMS-N2-61490', 'MI20-EMS-N2-62970', 'MI20-EMS-N2-60980', 'MI20-EMS-N2-506083', 'MI20-EMS-N2-62930', 'MI20-EMS-N2-61690', 'MI20-EMS-N2-61440', 'MI20-EMS-N2-510102', 'MI20-EMS-N2-119858', 'MI20-EMS-N2-115460', 'MI20-EMS-N2-219470', 'MI20-EMS-N2-521730', 'MI20-EMS-N2-62730', 'MI20-EMS-N2-603152', 'MI20-EMS-N2-60480', 'MI20-EMS-N2-96320', 'MI20-EMS-N2-706000', 'MI20-EMS-N2-503230', 'MI20-EMS-N2-91868', 'MI20-EMS-N2-923291', 'MI20-EMS-N2-61970', 'MI20-EMS-N2-505866', 'MI20-EMS-N2-508548', 'MI20-EMS-N2-603072', 'MI20-EMS-N2-114080', 'MI20-EMS-N2-98142', 'MI20-EMS-N2-72260', 'MI20-EMS-N2-121758', 'MI20-EMS-N2-61830', 'MI20-EMS-N2-528311', 'MI20-EMS-N2-71760', 'MI20-EMS-N2-219662', 'MI20-EMS-N2-508524', 'MI20-EMS-N2-71810', 'MI20-EMS-N2-96960', 'MI20-EMS-N2-93740', 'MI20-EMS-N2-60700', 'MI20-EMS-N2-526562', 'MI20-EMS-N2-603752', 'MI20-EMS-N2-90906', 'MI20-EMS-N2-503440', 'MI20-EMS-N2-525620', 'MI20-EMS-N2-111727', 'MI20-EMS-N2-97596', 'MI20-EMS-N2-521221', 'MI20-EMS-N2-61990', 'MI20-EMS-N2-91127', 'MI20-EMS-N2-119720', 'MI20-EMS-N2-96646', 'MI20-EMS-N2-522390', 'MI20-EMS-N2-119946', 'MI20-EMS-N2-92433', 'MI20-EMS-N2-520498', 'MI20-EMS-N2-104600', 'MI20-EMS-N2-508532', 'MI20-EMS-N2-528341', 'MI20-EMS-N2-120319', 'MI20-EMS-N2-94710', 'MI20-EMS-N2-527881', 'MI20-EMS-N2-527119', 'MI20-EMS-N2-104490', 'MI20-EMS-N2-506500', 'MI20-EMS-N2-61290', 'MI20-EMS-N2-61650', 'MI20-EMS-N2-62650', 'MI20-EMS-N2-527511', 'MI20-EMS-N2-60540', 'MI20-EMS-N2-62360', 'MI20-EMS-N2-506613', 'MI20-EMS-N2-528154', 'MI20-EMS-N2-526702', 'MI20-EMS-N2-60920', 'MI20-EMS-N2-506499', 'MI20-EMS-N2-505966', 'MI20-EMS-N2-505652', 'MI20-EMS-N2-526458', 'MI20-EMS-N2-115240', 'MI20-EMS-N2-300144', 'MI20-EMS-N2-62630', 'MI20-EMS-N2-522230', 'MI20-EMS-N2-521190', 'MI20-EMS-N2-100930', 'MI20-EMS-N2-62710', 'MI20-EMS-N2-508556', 'MI20-EMS-N2-118755', 'MI20-EMS-N2-600502', 'MI20-EMS-N2-120151', 'MI20-EMS-N2-522340', 'MI20-EMS-N2-71780', 'MI20-EMS-N2-120558', 'MI20-EMS-N2-62110', 'MI20-EMS-N2-61950', 'MI20-EMS-N2-528251', 'MI20-EMS-N2-94990', 'MI20-EMS-N2-115300', 'MI20-EMS-N2-110530', 'MI20-EMS-N2-62040', 'MI20-EMS-N2-62950', 'MI20-EMS-N2-499998', 'MI20-EMS-N2-63100', 'MI20-EMS-N2-524890', 'MI20-EMS-N2-107460', 'MI20-EMS-N2-118950', 'MI20-EMS-N2-91343', 'MI20-EMS-N2-92780', 'MI20-EMS-N2-113120', 'MI20-EMS-N2-525720', 'MI20-EMS-N2-62530', 'MI20-EMS-N2-508032', 'MI20-EMS-N2-400990', 'MI20-EMS-N2-70160', 'MI20-EMS-N2-62790', 'MI20-EMS-N2-97700', 'MI20-EMS-N2-109120', 'MI20-EMS-N2-111598', 'MI20-EMS-N2-508394', 'MI20-EMS-N2-499985', 'MI20-EMS-N2-527044', 'MI20-EMS-N2-62170', 'MI20-EMS-N2-499981', 'MI20-EMS-N2-62610', 'MI20-EMS-N2-60600', 'MI20-EMS-N2-923021', 'MI20-EMS-N2-521170', 'MI20-EMS-N2-522130', 'MI20-EMS-N2-60880', 'MI20-EMS-N2-104830', 'MI20-EMS-N2-99530', 'MI20-EMS-N2-94910', 'MI20-EMS-N2-521030', 'MI20-EMS-N2-116452', 'MI20-EMS-N2-523860', 'MI20-EMS-N2-524400', 'MI20-EMS-N2-70150', 'MI20-EMS-N2-528045', 'MI20-EMS-N2-523640', 'MI20-EMS-N2-62320', 'MI20-EMS-N2-60560', 'MI20-EMS-N2-603232', 'MI20-EMS-N2-499600', 'MI20-EMS-N2-60900', 'MI20-EMS-N2-525310', 'MI20-EMS-N2-61020', 'MI20-EMS-N2-600402', 'MI20-EMS-N2-114760', 'MI20-EMS-N2-525370', 'MI20-EMS-N2-70170', 'MI20-EMS-N2-61870', 'MI20-EMS-N2-97450', 'MI20-EMS-N2-521558', 'MI20-EMS-N2-72050', 'MI20-EMS-N2-98672', 'MI20-EMS-N2-70118', 'MI20-EMS-N2-98910', 'MI20-EMS-N2-300257', 'MI20-EMS-N2-525190', 'MI20-EMS-N2-523200', 'MI20-EMS-N2-62490', 'MI20-EMS-N2-120608', 'MI20-EMS-N2-113540', 'MI20-EMS-N2-99150', 'MI20-EMS-N2-62270', 'MI20-EMS-N2-62230', 'MI20-EMS-N2-60440', 'MI20-EMS-N2-121898', 'MI20-EMS-N2-505660', 'MI20-EMS-N2-525990', 'MI20-EMS-N2-61890', 'MI20-EMS-N2-527963', 'MI20-EMS-N2-525840', 'MI20-EMS-N2-61910', 'MI20-EMS-N2-61350', 'MI20-EMS-N2-523380', 'MI20-EMS-N2-62340', 'MI20-EMS-N2-523720', 'MI20-EMS-N2-525060', 'MI20-EMS-N2-114670', 'MI20-EMS-N2-62190', 'MI20-EMS-N2-120868', 'MI20-EMS-N2-523290', 'MI20-EMS-N2-499989', 'MI20-EMS-N2-60960', 'MI20-EMS-N2-528114', 'MI20-EMS-N2-107230', 'MI20-EMS-N2-62550', 'MI20-EMS-N2-119514', 'MI20-EMS-N2-100240', 'MI20-EMS-N2-527933', 'MI20-EMS-N2-106814', 'MI20-EMS-N2-90414', 'MI20-EMS-N2-112540', 'MI20-EMS-N2-520678', 'MI20-EMS-N2-121098', 'MI20-EMS-N2-92530', 'MI20-EMS-N2-106982', 'MI20-EMS-N2-94964', 'MI20-EMS-N2-113640', 'MI20-EMS-N2-113330', 'MI20-EMS-N2-115894', 'MI20-EMS-N2-525430', 'MI20-EMS-N2-63010', 'MI20-EMS-N2-61610', 'MI20-EMS-N2-119304', 'MI20-EMS-N2-71950', 'MI20-EMS-N2-61730', 'MI20-EMS-N2-604062', 'MI20-EMS-N2-114072', 'MI20-EMS-N2-98972', 'MI20-EMS-N2-62870', 'MI20-EMS-N2-60760', 'MI20-EMS-N2-525520', 'MI20-EMS-N2-110950', 'MI20-EMS-N2-523900', 'MI20-EMS-N2-62850', 'MI20-EMS-N2-61670', 'MI20-EMS-N2-62690', 'MI20-EMS-N2-109280', 'MI20-EMS-N2-523430', 'MI20-EMS-N2-119544', 'MI20-EMS-N2-60660', 'MI20-EMS-N2-117760', 'MI20-EMS-N2-62590', 'MI20-EMS-N2-108168', 'MI20-EMS-N2-120568', 'MI20-EMS-N2-91551', 'MI20-EMS-N2-90514', 'MI20-EMS-N2-60460', 'MI20-EMS-N2-525660', 'MI20-EMS-N2-62450', 'MI20-EMS-N2-300211', 'MI20-EMS-N2-60580', 'MI20-EMS-N2-525560', 'MI20-EMS-N2-521510', 'MI20-EMS-N2-62150', 'MI20-EMS-N2-528281', 'MI20-EMS-N2-108410', 'MI20-EMS-N2-93950', 'MI20-EMS-N2-62250', 'MI20-EMS-N2-60640', 'MI20-EMS-N2-522800', 'MI20-EMS-N2-96450', 'MI20-EMS-N2-121018', 'MI20-EMS-N2-104960', 'MI20-EMS-N2-119122', 'MI20-EMS-N2-603662', 'MI20-EMS-N2-109760', 'MI20-EMS-N2-524680', 'MI20-EMS-N2-120648', 'MI20-EMS-N2-603462', 'MI20-EMS-N2-219566', 'MI20-EMS-N2-105680', 'MI20-EMS-N2-98712', 'MI20-EMS-N2-61750', 'MI20-EMS-N2-523110', 'MI20-EMS-N2-60500', 'MI20-EMS-N2-503330', 'MI20-EMS-N2-93810', 'MI20-EMS-N2-526832', 'MI20-EMS-N2-61530', 'MI20-EMS-N2-523780', 'MI20-EMS-N2-61710', 'MI20-EMS-N2-524430', 'MI20-EMS-N2-113260', 'MI20-EMS-N2-91850', 'MI20-EMS-N2-61770', 'MI20-EMS-N2-922742', 'MI20-EMS-N2-98980', 'MI20-EMS-N2-112450', 'MI20-EMS-N2-525760', 'MI20-EMS-N2-90380', 'MI20-EMS-N2-93600', 'MI20-EMS-N2-525400', 'MI20-EMS-N2-523980', 'MI20-EMS-N2-407520', 'MI20-EMS-N2-112730', 'MI20-EMS-N2-98590', 'MI20-EMS-N2-62210', 'MI20-EMS-N2-104370', 'MI20-EMS-N603612', 'MI20-EMS-N2-219710', 'MI20-EMS-N2-63050', 'MI20-EMS-N2-71790', 'MI20-EMS-N2-113000', 'MI20-EMS-N2-70115', 'MI20-EMS-N2-114390', 'MI20-EMS-N2-508516', 'MI20-EMS-N2-90114', 'MI20-EMS-N2-121988', 'MI20-EMS-N2-524830', 'MI20-EMS-N2-63070', 'MI20-EMS-N2-117050', 'MI20-EMS-N2-700000', 'MI20-EMS-N2-603892', 'MI20-EMS-N2-120504', 'MI20-EMS-N2-70072', 'MI20-EMS-N2-61470', 'MI20-EMS-N2-708008', 'MI20-EMS-N2-111260', 'MI20-EMS-N2-63090', 'MI20-EMS-N2-510078', 'MI20-EMS-N2-94850', 'MI20-EMS-N2-525340', 'MI20-EMS-N2-93530', 'MI20-EMS-N2-60860', 'MI20-EMS-N2-60800', 'MI20-EMS-N2-62990', 'MI20-EMS-N2-603822', 'MI20-EMS-N2-111288', 'MI20-EMS-N2-111740', 'MI20-EMS-N2-62400', 'MI20-EMS-N2-99210', 'MI20-EMS-N2-63080', 'MI20-EMS-N2-116290', 'MI20-EMS-N2-96870', 'MI20-EMS-N2-92990', 'MI20-EMS-N2-91990', 'MI20-EMS-N2-527821', 'MI20-EMS-N2-114122', 'MI20-EMS-N2-62670', 'MI20-EMS-N2-62810', 'MI20-EMS-N2-219614', 'MI20-EMS-N2-922951', 'MI20-EMS-N2-110860', 'MI20-EMS-N2-502010', 'MI20-EMS-N2-524062', 'MI20-EMS-N2-62510', 'MI20-EMS-N2-60620', 'MI20-EMS-N2-525960', 'MI20-EMS-N2-525000', 'MI20-EMS-N2-105570', 'MI20-EMS-N2-508540', 'MI20-EMS-N2-104190', 'MI20-EMS-N2-120528', 'MI20-EMS-N2-117310', 'MI20-EMS-N2-62380', 'MI20-EMS-N2-112050', 'MI20-EMS-N2-60780', 'MI20-EMS-N2-499870', 'MI20-EMS-N2-72160', 'MI20-EMS-N2-703000', 'MI20-EMS-N2-300855', 'MI20-EMS-N2-113950', 'MI20-EMS-N2-94520', 'MI20-EMS-N2-523550', 'MI20-EMS-N2-521310', 'MI20-EMS-N2-93130', 'MI20-EMS-N2-120052', 'MI20-EMS-N2-106170', 'MI20-EMS-N603482', 'MI20-EMS-N2-107900', 'MI20-EMS-N2-96734', 'MI20-EMS-N2-510176', 'MI20-EMS-N2-120478', 'MI20-EMS-N2-111834', 'MI20-EMS-N2-525460', 'MI20-EMS-N2-119944', 'MI20-EMS-N2-923071', 'MI20-EMS-N2-500003', 'MI20-EMS-N2-61570', 'MI20-EMS-N2-62750', 'MI20-EMS-N2-62100', 'MI20-EMS-N2-522520', 'MI20-EMS-N2-524920', 'MI20-EMS-N2-105930', 'MI20-EMS-N2-111330', 'MI20-EMS-N2-116790', 'MI20-EMS-N2-61630', 'MI20-EMS-N2-60680', 'MI20-EMS-N2-60740', 'MI20-EMS-N2-70071', 'MI20-EMS-N2-93190', 'MI20-EMS-N2-603172', 'MI20-EMS-N2-61590', 'MI20-EMS-N2-525900', 'MI20-EMS-N2-62430', 'MI20-EMS-N2-108004', 'MI20-EMS-N603532', 'MI20-EMS-N2-94160', 'MI20-EMS-N2-62770', 'MI20-EMS-N2-62470', 'MI20-EMS-N2-923111', 'MI20-EMS-N2-119856', 'MI20-EMS-N2-522710', 'MI20-EMS-N2-61930', 'MI20-EMS-N2-70073', 'MI20-EMS-N2-120362', 'MI20-EMS-N2-525160', 'MI20-EMS-N2-70432', 'MI20-EMS-N2-72360', 'MI20-EMS-N2-99380', 'MI20-EMS-N2-114044', 'MI20-EMS-N2-60840', 'MI20-EMS-N2-115740', 'MI20-EMS-N2-62890', 'MI20-EMS-N2-527903', 'MI20-EMS-N2-91002', 'MI20-EMS-N2-521260', 'MI20-EMS-N2-506079', 'MI20-EMS-N2-527451', 'MI20-EMS-N2-62300', 'MI20-EMS-N2-98962', 'MI20-EMS-N2-603332', 'MI20-EMS-N2-60720', 'MI20-EMS-N2-104710', 'MI20-EMS-N2-120054', 'MI20-EMS-N2-113990', 'MI20-EMS-N2-61810', 'MI20-EMS-N2-121598', 'MI20-EMS-N2-503010', 'MI20-EMS-N2-510126', 'MI20-EMS-N2-120688', 'MI20-EMS-N2-61550', 'MI20-EMS-N2-94230', 'MI20-EMS-N2-63130', 'MI20-EMS-N2-120395', 'MI20-EMS-N2-98110', 'MI20-EMS-N2-91190', 'MI20-EMS-N2-92920', 'MI20-EMS-N2-70140', 'MI20-EMS-N2-91260', 'MI20-EMS-N2-499994', 'MI20-EMS-N2-92001', 'MI20-EMS-N2-112120', 'MI20-EMS-N2-111565', 'MI20-EMS-N2-119492', 'MI20-EMS-N2-98724', 'MI20-EMS-N2-120768', 'MI20-EMS-N2-61510', 'MI20-EMS-N2-600302', 'MI20-EMS-N2-510054', 'MI20-EMS-N2-400300', 'MI20-EMS-N2-508564', 'MI20-EMS-N2-95770', 'MI20-EMS-N2-119502', 'MI20-EMS-N2-510152', 'MI20-EMS-N2-60940', 'MI20-EMS-N2-524040', 'MI20-EMS-N2-115830', 'MI20-EMS-N2-121318', 'MI20-EMS-N2-120549', 'MI20-EMS-N2-219518']
for scaffolds_file in scaffolds_files:
    scaffold_path = os.path.join(scaffolds_path1, scaffolds_file)
    scaffold = np.load(scaffold_path, allow_pickle=True)[()]
    for k, chain in scaffold.items():
        if chain:
            encoded_tasks = chain.split(node_delimiter)
            tasks = [nodes_decoder[t] for t in encoded_tasks]
            for terminal in missing_terminals:
                if terminal in tasks:
                    print(30 * '-------')
                    print('terminal not in chains:', terminal)
                    print(scaffold)
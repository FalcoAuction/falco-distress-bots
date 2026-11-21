"""Exact newspaper excerpts adjudicated for the Tennessee tax categories.

Normalize whitespace only. A longer notice, including an added parcel exhibit,
requires its own review even when the caption and newspaper are identical.
"""
import hashlib


_REVIEWED = {
    "anderson": {
        "42ab9ea92a6a11bdbe91d4d92146b1b64e64a02b189019ab80ae9550b73069d4": "Roane County municipal tax suit published in Anderson",
        "f93e3a4f47fed4263b3294aefb8942a12ee5955e66da1311d3bb328a8bb59e6c": "Roane County municipal tax suit published in Anderson",
        "80409f2a4d51d40ce108374c1857fab106a334846d9723b5a7f9e9e1ade824fe": "DOE environmental public-comment period",
    },
    "bedford": {
        "2b67365cef3582d8dec938ce05d942fb188c5d2e6f7599de374430359959f183": "Election or ballot publication",
        "6b2c87d0605b1b18f96cb2b8f395ae2e136b0d9b03ceb7e5c24d15034f643f50": "Government budget or appropriation",
        "f3c249c7ab5624c755e4924444a4e4f932caa14b73dc453862d9afadb1ff9c8e": "Public meeting or agenda",
    },
    "benton": {
        "b14b633c02fd28b94848b52548353ce081ba57dcded86050c5e81040f7b3c7ba": "Juvenile proceeding",
        "d95646ee0ff6b7e260c4cef4b23eca2c4e1f0afe8edaf610d7a1a98ae0468296": "Public meeting or agenda",
    },
    "bradley": {
        "cb1810cebe7a2123529539fa9a3fc801f86532cb6e23d2594fc1cc5a07ce6b8e": "Scheduled municipal or county public hearing",
        "2c53d63309ac37f8f42cef3058e65ab2a42e2f445b0fea3cda2044e6eba6e9b9": "Scheduled municipal or county public hearing",
        "1f6d5bea0906a103700e8b0cdff33a0bd6dc7fee2b39b5c7baf81a406102ec3c": "General assessment notification or inspection",
        "ded2a42d408a353b4b74cfaeb35b1b33b48b665a307246750982434313cbd3f5": "Scheduled municipal or county public hearing",
        "69a18245ea58a8046513b7d2d47480de490dd99001589cba6b69797fffeae97a": "Mortgage trustee sale",
        "9cdd2547b2a942540b76d9c2f9505d1154b24768cc569632bce3847d3d64eaa8": "Government budget or appropriation",
        "ed414c8c897a86b4e92c5de916cd2f771f0b157caf42663cf8aea2c2db1dd21e": "Government budget or appropriation",
        "627d1db9c18e81706bc905f1956c97dc6390aaca5e0f07e78fcfd617c67e892a": "Government budget or appropriation",
    },
    "campbell": {
        "4edacd7ff721e8fc819bd544efc72f89fae11fe974626fc08022a20a2cbb0c03": "Municipal demolition order",
    },
    "cannon": {
        "bfdd50537c312b88d53ee615dbb7b86f3aec823edac2625cf630e9e615d4ee2d": "Government budget or appropriation",
        "4651341cb016b035034de817bb462148072ad5800c09e1714aaf787564d6d1fe": "Public meeting or agenda",
        "f8ff7497c6ead0499b49e25508a24a618e7d6a75a0fd985b7196f0323024faf5": "Public meeting or agenda",
        "0a510bcc480c6811593eda014d13ab1637e9b228a654b0083b870b5ec4088f8f": "Public meeting or agenda",
        "7bd42eea39457a575b9148c62f19111fab8c603550e02749887fa9f81699082c": "Public meeting or agenda",
        "82cbd6cfe944b1e878e43ff9722a788906b471596b269c2899c80e0782200c28": "Public meeting or agenda",
    },
    "carter": {
        "b9798f90e4cfc087f20bde52b7424f53c7f873d5826c54c77c182693c5c23baa": "General property-tax billing or collection announcement",
        "0ac9394f44d1754376d9906a8882002d2b4aedbde5c07eb24b684b7f44704390": "Mortgage trustee sale",
        "e096855b0ad7ecc74d2ec48c360f03fae4a55695101a2d8e56a388af4605c7ce": "General property-tax billing or collection announcement",
        "a8f58f1ff8c284a52e8502524390dd3a024b6605eac4f769fe396bf40f8c68d0": "General property-tax billing or collection announcement",
    },
    "claiborne": {
        "948ee08713d7a8762712349e2945e81a0d1cc83a95c091cf625814b9b2303bb6": "Annexation or zoning hearing",
    },
    "cocke": {
        "c400eff7eef01fc6e68b086298868415bbc093960aa1e1d065291834a1d52768": "Election or ballot publication",
        "7973ebba34be5edf9e196f3739431752218e09d378f9aa498a2187f504bb0f14": "Mortgage trustee sale",
    },
    "cumberland": {
        "a3ecfb8fdea5ea87820742983d5077c730c07087f0833895119b05925a6c5f35": "Completed Fairfield Glade county resale, deed 1759/1353",
        "cbb28e19315404780a76af278f9303b7321982a97b05ad6ff2f3eaa2f9f08cda": "General property-tax billing or collection announcement",
        "cc8c2cc08d23d2f37490811debcc8bf9ce2a0f5331457bac9eb0e567b78f5165": "Government budget or appropriation",
    },
    "decatur": {
        "67f455774847e1636083a24e03c34fa8f945625545964c78fa49a4075c5db2e3": "Certified tax rate hearing",
    },
    "dekalb": {
        "887e00d6596638f06bbc656964e1ba73c01c3365b8ada0355fd9ea9e99313a8e": "Court-ordered land sale in named private-party action",
        "261bcdfaecc9fce7bc38880794dfd4dbdd087d3f677c6ffb2a5f33c336223828": "Agricultural lease solicitation",
        "bc3b508abc70e705d3233f6162a9cc620f84fc3e55d4d799093ec9da14e82e51": "Election or ballot publication",
        "cfbf65c7d764fba6c498fc896bfc514bd16bc362652f732c0f5b4865213ffabd": "General property-tax billing or collection announcement",
    },
    "fayette": {
        "b0b124452da73506cb7fb7aad88b98a6474561d43a62c16a2fb4bae52d713e75": "Election or ballot publication",
        "391f161050f196c83898742a2b7e011e2084b9879df449388d38d71694709e68": "Government budget or appropriation",
        "6b764b6eb0ab59bfe8372f1dca62bd5a13c14dee8f14db3630ceeb6e40f70b02": "Oakland code enforcement correspondence",
        "aa84cc3cd4546336b8e71645e60bf7a89e6e06b9bbd52306ed88131cb9e1e01c": "Oakland code enforcement correspondence",
        "db81e61990043de81165c23841084e2d6680d11e9426ca624fba0998a6503fd4": "Oakland code enforcement correspondence",
        "075fe64e8481107aae74356c1564ae88b15d0a2df6fa2b1269c49be7dab37fdc": "Government budget or appropriation",
    },
    "fentress": {
        "4029368ad3ee83b2bfeb498c0420ab00e72ea46d6abb3bf91e64070f0c8a1dfd": "Probate proceeding or private estate property",
        "cca0fdebc35ffe1ca5774155b19344696fe46872c6954029bec382ddbf6b49d0": "Abandoned aircraft disposal",
    },
    "gibson": {
        "58fd9b8b9bedcc19cdcf958a298604bc32e5a6ee9b64bb1e1c2e78977cba1b46": "Probate proceeding or private estate property",
        "dccfac8279a0022033d9a0a157ec2786058865fc248fc4ae497cb47ccd18f850": "Annexation or zoning hearing",
        "4894509b8cdb5b1446e0e0e4ed38d445f677f637ebef2e56192ae58611a088af": "Government budget or appropriation",
    },
    "giles": {
        "81b2eece0bf4010e233034431e11aef81e25f4bdb62cf062cb75922e6c0e83ca": "General property-tax billing or collection announcement",
        "44405d79b62f14ef66f78e545e2796937ad2973c7298c02e2f8b67969d5eca9d": "Election or ballot publication",
        "54da0af70ca8b0308a8ffdebbd934da1e7f10f8e2e244d5084dfdb681bd509f5": "Annexation or zoning hearing",
    },
    "grainger": {
        "d227cd77f85f235df1e0653affcc166bd274e7b2bea0c62d5528859bd58d0a66": "Election or ballot publication",
    },
    "greene": {
        "e93a15e2c78f510ddde0d753112c166b8e0683fec8dd4ce03cea49c45a4bcf69": "Estate auction for heirs",
        "b394c3f8b4a2d3881c71dc0467f4ec24c8dc94aaa1932e3c0585c648682236f9": "Mortgage trustee sale",
        "8961f3d6adc677e7243387f100e61ea3904639c472741eaadbaa0799dcde0223": "Election or ballot publication",
    },
    "grundy": {
        "246374bfa25f72dca50636141c486f402219caa9fb0bb0023ea516d7d485a3e5": "Certified tax rate hearing",
    },
    "hamblen": {
        "c2993e16c0eca5e08bbd2ee328210a6ed2149c245382adc82e4ebc1d6c707555": "Probate proceeding or private estate property",
        "f9a1759fee12c3bbee19a5acf8a08ca2e719d2fc382ec154d9dc5fb6eac0bd46": "Scheduled municipal or county public hearing",
        "e974d67be10804485763d5ac658d59a47901203539f512d4ddfc71f90cdac7cf": "Probate proceeding or private estate property",
    },
    "hancock": {
        "3682fc5728f2840b8ddd8aab866eec7726ff169b18cc85f78890dc82b4d16a48": "Probate proceeding or private estate property",
    },
    "haywood": {
        "567550fd848ef92601408cc79a177d616a7ebe29f041104320d9c1d20f1da919": "Government budget or appropriation",
        "f4328831412fe6818d8dfc8ee153ac7b8c9dc30208ab7016fe0d2a8c1bba0174": "General assessment notification or inspection",
    },
    "hickman": {
        "052b14951b078845b8bb999e15705d3584a04e69d2e3debf8d7923e9117d1280": "Certified tax rate hearing",
    },
    "humphreys": {
        "f7f0fdc4f53043711701a03bdfc08689cf3ef08c231667417805256588c736b7": "Certified tax rate hearing",
        "ab34698c9fda65fb454e890a8b5065cafeec0395af85764a78c06473eed3be4e": "Public meeting or agenda",
    },
    "lewis": {
        "922d673fad85da19cbba391e3820050ae2856cb9a1780111ea911efa8eecda8d": "Public meeting or agenda",
        "ffc830bddca3bb80d854c83e4f71959d0794fa69b0f30769d968fd0473947a04": "Treasury unclaimed-property program",
        "feb202221dcf831712e7eb1d4f07f1e4af5ef42e544a2313ec5254a67d740a3e": "Grand jury meeting",
    },
    "loudon": {
        "d04da30429f6370e63a34fe5f0ea5228dd91e8554955bf07bcb37a44f42699cb": "Election or ballot publication",
    },
    "macon": {
        "c843aa00065282f83f68255d6ad7735205c943d54970f96692e2b3d94f4a3a44": "Certified tax rate hearing",
    },
    "marshall": {
        "1b8da7b14d6657670b9a8e3de55d57514c74ce8746cb8fc44f6a5a716ff6c9ca": "Public meeting or agenda",
    },
    "mcminn": {
        "ea7897f696a056e4153e8191d6b5ff87b66be043f25fe211561a6500c40784b1": "Election or ballot publication",
        "4a8b6192a7a26139ebc7ff8fd24cd58bc94a5963a015463d903a975745503967": "Election or ballot publication",
        "8495f362383ec57fc0daebbd1f1d6a703b604bc85c2f4286f230e41a12ef2c28": "Estate auction for heirs",
    },
    "monroe": {
        "623bec9ec66dd90640a848fea99944c625e5472409507ab4b4cf375afead00f3": "Vehicle seizure with altered VIN",
    },
    "morgan": {
        "92fc494d1f40c65417955c55bd0243a0c383154b72131b9207e9b480b29b6061": "Government budget or appropriation",
    },
    "obion": {
        "368baa5acc29bff2183b5aaf7662a3df15517567b06c1188f271e9b8c25b537c": "Annexation or zoning hearing",
        "3aeb8f02423381132bcd8fd45cbe338e88c429ae4792d045bb100f26bfd3d5cc": "Annexation or zoning hearing",
    },
    "overton": {
        "18bc1de4ee80179e45972508c21c8e9673e2170e32f826ebb4bc99b4ad90c4e5": "Private change of ownership and access notice",
    },
    "pickett": {
        "ab4ba1fa074d7d76462b8e0665677df1c0bc4176766fe53541ec68916cabda48": "Court-ordered land sale in named private-party action",
    },
    "rhea": {
        "2b04f131a5fbbda323a3d6d395d750a3299826fb3090bcc072c71743c33c1a26": "Mortgage trustee sale",
    },
    "scott": {
        "0ffce316e858b273483de9f5d9eb01585a8cb29211ce7f0c8e1c582666478d56": "Annexation or zoning hearing",
        "12060cb97f9bc52a3a2a9fa7082e6c0ffc124645e5d66af09dcb4b2fb457ef3c": "Municipal surplus sale",
    },
    "sequatchie": {
        "e4df6d42e72f442df0113df8d59d2c7175bca9d5642112fe23fc88bd206e720c": "Election or ballot publication",
        "a25c2f366c91add2e8d377d2022c1c89686a053823db820b51cd149afe594506": "General assessment notification or inspection",
        "18c6a792100a09c737746916bac0ce58eaf2cf79e7207c1542d25ff96e2f155d": "Government budget or appropriation",
    },
    "smith": {
        "dd52328a7bb7f5c72eb4f025129ac7c9cc8d40242412d7c67f26ac45533b66b0": "Halloween conduct ordinance",
        "464ef7c1f577bd6d82e8929027642e9ba2968e09109b8cfed229334b6e62a829": "Probate proceeding or private estate property",
        "438bd6d216a85e972a8235adf3fdfb9436f5fc12a090995d2c96872825c33f6f": "Probate proceeding or private estate property",
        "f83de6288183741c4edd0680bdf6bb33ee73eb3f972b33a9fdefa1637c59f6b0": "Scheduled municipal or county public hearing",
    },
    "tipton": {
        "e9d730b285c9b4f3b05ac9379db9af4fc9c853d1fbd8f07d041bedf22d03ff7a": "Government budget or appropriation",
        "ff61e96b584fe5b0b698b33dfd57f027bd3d1d26f5e43a73492199a6d558e9d3": "Election or ballot publication",
    },
    "unicoi": {
        "8144b40479fc191730509bee68cbdfc83f9ccd63c0e4625ad1c7681653857594": "Government budget or appropriation",
    },
    "union": {
        "1a519b5442e51102e13a06be0f1515dd1c895f0ef6320825c35ebf2a8f8bcd94": "Court-ordered land sale in named private-party action",
        "a64377725b6c130526faea28aeb94a982a0d6ffbc002a797e6f9eb5ed32e7808": "Probate proceeding or private estate property",
        "e236462c4d0d23657d82b0f54717bd7534e8a2bf78f6bde4e7d3613eea811da6": "Court-ordered land sale in named private-party action",
        "21ad34294246168d4d8a53f5e9c0342d779ca42d891d5c48762dd2b003377f06": "Probate proceeding or private estate property",
        "8bdc2324718906cf3ad6bc974a776f33ca14a756ac0460cf451dc086776c61a5": "Court-ordered land sale in named private-party action",
        "b53557113ce1e35ecd42ed5eb51068b8e03fa7e3dc58a565ad0eb1f4ff128cae": "Probate proceeding or private estate property",
        "9fe831b6b20738d313c36e5626696469750348dc8d780853d4c2f6da6408f485": "Mortgage trustee sale",
        "ccbb3c931535e67e42116b437b23e5babdfb57bd214a41b6b45710ee626d7b6d": "Probate proceeding or private estate property",
        "2888d2747516b97f92ff44e0ae1501f777bb4838ce5e2bc628133c68ba393a4b": "Probate proceeding or private estate property",
    },
    "weakley": {
        "660cf0c827ba47aee21a0be092934bf142204b5e614d904e9f3ce18b7f062aa1": "Annexation or zoning hearing",
        "71544b9db1647fd5c9af05fa33ef3aae7b4d097817ba49ce9f2ac73317435f9c": "Annexation or zoning hearing",
        "36ec95b7a47d0d0788653ca92af9556ccb6505ab5b7d1e12e6b5864d9bced517": "General property-tax billing or collection announcement",
        "b8ce963f1321d28adba4242b4bf5a9fe3f33d66ef6c2dd2a164e20f036998a27": "Montgomery County September 17 sale published in Weakley",
        "ede428ff81db32d0fb590e35fcc3f73a0b3deba7ba5db9190f698a5e6e496b85": "Public meeting or agenda",
    },
    "white": {
        "45d4c66c0cb716b4ecedab6576eee8050c3e41316936d5c79d34a7614d696cb2": "Probate proceeding or private estate property",
    },
}


def disposition(county, excerpt):
    reviewed = _REVIEWED.get(county)
    if reviewed:
        normalized = " ".join(excerpt.split()).encode("utf-8")
        return reviewed.get(hashlib.sha256(normalized).hexdigest())
    return None

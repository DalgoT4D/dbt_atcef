Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
```
dbt_atcef
├─ .git
│  ├─ COMMIT_EDITMSG
│  ├─ FETCH_HEAD
│  ├─ HEAD
│  ├─ ORIG_HEAD
│  ├─ config
│  ├─ description
│  ├─ hooks
│  │  ├─ applypatch-msg.sample
│  │  ├─ commit-msg.sample
│  │  ├─ fsmonitor-watchman.sample
│  │  ├─ post-update.sample
│  │  ├─ pre-applypatch.sample
│  │  ├─ pre-commit.sample
│  │  ├─ pre-merge-commit.sample
│  │  ├─ pre-push.sample
│  │  ├─ pre-rebase.sample
│  │  ├─ pre-receive.sample
│  │  ├─ prepare-commit-msg.sample
│  │  ├─ push-to-checkout.sample
│  │  └─ update.sample
│  ├─ index
│  ├─ info
│  │  └─ exclude
│  ├─ objects
│  │  ├─ 00
│  │  │  └─ cf58d9fc29feb89d573cd774b88b0a30675cac
│  │  ├─ 01
│  │  │  └─ 2f9796c6d90a2bd25892aedb6f9d3ee16fe2d2
│  │  ├─ 02
│  │  │  └─ 78c32edb2078b95a98f5507569eede981d05fc
│  │  ├─ 05
│  │  │  ├─ 000371723a6c84ccc8b790c913affe0c28e339
│  │  │  └─ 5a8ff0bfbab3f3689dfb0bbc94a9bf8984dc2e
│  │  ├─ 06
│  │  │  └─ 06bce3a2fe4cec1a7ef96afae7fe051e8f3551
│  │  ├─ 08
│  │  │  ├─ 1e98a5dadf8434d0377f24032f32827f0f0b23
│  │  │  └─ e8698abb97c7620829214fa5819a13d1d8d19b
│  │  ├─ 0a
│  │  │  └─ d48598a5a18c3af7f4b6d338325e3c3540a73e
│  │  ├─ 0b
│  │  │  └─ e7538c7f57d7d72ed7ce29e98314f4ccf49dcc
│  │  ├─ 0d
│  │  │  └─ 0da4d7f674152623874c1efb81aa7c03673ef4
│  │  ├─ 0f
│  │  │  └─ 7b742b660c89f4fb30e155c31ceee1cc070daa
│  │  ├─ 10
│  │  │  ├─ 1b46b618648a3175b10cbf9fd241065419d70c
│  │  │  └─ b23a851386048f9ed4909f0fd3e73e34453c04
│  │  ├─ 12
│  │  │  ├─ e39f955688ca938d78479fd9cf286ad8b33886
│  │  │  └─ ed463ee1972f9b324ed04a9f3488680b5f4af6
│  │  ├─ 14
│  │  │  └─ 652e4874b592a665570b8e665336c33458eee8
│  │  ├─ 18
│  │  │  └─ e7f76b2de31ffdbe3d0fb9e59abd8f999554ee
│  │  ├─ 19
│  │  │  └─ 77595413dae6179958300ed15659d239b2bbd6
│  │  ├─ 1a
│  │  │  └─ d901586464d15b3d7d8578d51ad9e15e55e8e8
│  │  ├─ 1b
│  │  │  └─ b0518304cb89e124c635db7a5f5d027b4a7e16
│  │  ├─ 1f
│  │  │  ├─ 4ea92c21c8492d4ba6574f81b15f9a8535a0db
│  │  │  └─ 9a441ec43aa326bd451ee881cfe4c277a604ab
│  │  ├─ 23
│  │  │  └─ 4f15e9bce727a200ecb23bbf3aebc620b1c7a3
│  │  ├─ 26
│  │  │  └─ 80fe61cfd8eea9efdcfc659e5cd9cfa4cfceda
│  │  ├─ 27
│  │  │  ├─ 26b936837600a5b5bd43a6dc3740195c98bc40
│  │  │  └─ e17a67d7c04ce9a1713f51e7fbfd51301f6671
│  │  ├─ 28
│  │  │  └─ 513b97641a6aac537b69e3aae30beaf3b1a334
│  │  ├─ 29
│  │  │  ├─ 038ec2d0f0b88da1db2c6e747c72c0704cba8b
│  │  │  └─ 76606049e5fc37d31cb80f924189d79f4d0324
│  │  ├─ 2a
│  │  │  └─ b9d7803a2036eed0705dbc696588186dba9bd0
│  │  ├─ 2b
│  │  │  └─ c982ae77b7eb478e6698ce607093f4c830b917
│  │  ├─ 2c
│  │  │  └─ 6a04fa03499a1f5677fb1589587c72edbbbfcc
│  │  ├─ 2e
│  │  │  └─ 8e7e551f1b5ca5cf45cf4e0b33c45cdea0726b
│  │  ├─ 2f
│  │  │  └─ d1238f9a5c9068043df6e167407cc0c00df59e
│  │  ├─ 30
│  │  │  └─ b8738bf3d2a61240e54cecba841e618832cb53
│  │  ├─ 31
│  │  │  └─ 0e7b0585971bd25344749f6422907e9695f2c8
│  │  ├─ 33
│  │  │  ├─ 9385edaa07d2538c0ff619d05c75218e29f88b
│  │  │  └─ a0df790aa5fa8e9b556fb648c0e05e51e4c079
│  │  ├─ 36
│  │  │  └─ 0e8cd95863809594e72f2bc3d94a6f6130a738
│  │  ├─ 37
│  │  │  └─ 08328cb805ed5119152fd37b68293ad99922cf
│  │  ├─ 3b
│  │  │  └─ 94469a5a12b600baaa44d0462544aec0ad17a8
│  │  ├─ 3c
│  │  │  ├─ 1a7161f565a72f845b6eaaaf2ac6245aa66d28
│  │  │  └─ 30515627b7869e6af4400c045fdddbc8882540
│  │  ├─ 3d
│  │  │  ├─ 64d7ead90ad87adbd61c9503c65048de542de9
│  │  │  ├─ 701e936ded83a1a180fd0f4500dbe19b53fc8b
│  │  │  └─ 9cffea4ed242a4826ead211ae222e72a5775c1
│  │  ├─ 40
│  │  │  ├─ 6a139ccf753cfa59693caea855a2135e73a1f7
│  │  │  └─ a3190469cc0a2848c1ee2eb4dd80513a69625f
│  │  ├─ 47
│  │  │  └─ c20cb702ee98cfe9ee565395e2072f55e32e09
│  │  ├─ 4a
│  │  │  └─ 888737fb807c1d2b57d17b65fafdcbd59fdafe
│  │  ├─ 4c
│  │  │  ├─ 775eae06ca4897ab01011701db45e75dabe17d
│  │  │  └─ e088133b951661fc23ac8a6053c0b80bdb1669
│  │  ├─ 4d
│  │  │  └─ 37c933ae39b383ccc5d8aeee103892f69851f7
│  │  ├─ 4e
│  │  │  └─ b058c4b0449c42c80a78819fc97dba327441bc
│  │  ├─ 50
│  │  │  └─ da8012cbaa0c815c82571f7aa2b3a7fcd753c1
│  │  ├─ 51
│  │  │  └─ 91d9e7bbf55ed4848f1477dddbbccdb5e35825
│  │  ├─ 53
│  │  │  └─ b6a4272a1888be1baa5e873c7dd1ccb7babc38
│  │  ├─ 54
│  │  │  ├─ 11e096295d53ab03bf6a8f480cb75915a281b2
│  │  │  └─ 35a28efe3e0c474b86507a217a8d25e219e8aa
│  │  ├─ 55
│  │  │  └─ 93c1fca669528ba7dd456010cfb569b8473650
│  │  ├─ 58
│  │  │  └─ f96ce4b20b55b0ab54b4c89f8f3da401454fc5
│  │  ├─ 5c
│  │  │  ├─ 14b46b95460b88a9fa3d4a377831d695ce81a9
│  │  │  ├─ 2d2febf34aea5d8003f85fb14eac8559d6099a
│  │  │  └─ b5db3749cb33b83177ba4c0e423909bb712a51
│  │  ├─ 60
│  │  │  └─ 1fb5150fded97e609e9d0cb6cac1d3b4efbd10
│  │  ├─ 61
│  │  │  └─ a59011c2f8b6b7e2180b3bd0baac70d770d8e0
│  │  ├─ 62
│  │  │  └─ f695deb37abcde043310eceb6d10855790290d
│  │  ├─ 63
│  │  │  └─ a644492b364011d93de970475be470013dc677
│  │  ├─ 64
│  │  │  ├─ 59fa84b33f94a5aad0505ff00b2f0d86ba1036
│  │  │  └─ bb7e00df64ea0f7bdd865b992b0d2d877b59a9
│  │  ├─ 66
│  │  │  └─ 54ba9b47a924669332e760a3689c54f6ed04be
│  │  ├─ 67
│  │  │  └─ 1614fdf3a61ffcf2f420e205dc7ab195bc5707
│  │  ├─ 69
│  │  │  ├─ a3707267db8fdc758eda19892a0f64c7adee0c
│  │  │  ├─ c653f78c31ed179823d057cecf41fd6670e3ec
│  │  │  └─ ea7c628043aee263be42e3688196db7679e8af
│  │  ├─ 6d
│  │  │  └─ 7e17784479366626db315c5a0bc7fa11e88bad
│  │  ├─ 6e
│  │  │  ├─ 3474454b9d63f8f6bf548c720bdb021e4a4ced
│  │  │  └─ 749a3389b114ec1c10d0b75746c969dab8110c
│  │  ├─ 6f
│  │  │  └─ 6d6919fdc5f9d6396ed57b74fe0915b87feed4
│  │  ├─ 71
│  │  │  └─ 29ebbfbc208146099aea9d3c13fc5fd26c15b9
│  │  ├─ 75
│  │  │  └─ cd59bacbffc25526dfdff72bf88fa5b5dea315
│  │  ├─ 78
│  │  │  ├─ 55deb9ab276ab446a6f4d9ef580fb028ca993f
│  │  │  └─ ae60fde7db4f7954b3243bb86fb44d62860ae5
│  │  ├─ 79
│  │  │  └─ 61786aa07d17176e2b7bd8365c4f1f8419ea42
│  │  ├─ 7f
│  │  │  └─ fceba3f95932cded9cafd86e6ed56d127d9e49
│  │  ├─ 81
│  │  │  ├─ 036d785cf195a8dbe062479ac4323be48a03cc
│  │  │  └─ 5a74ffd48b982208587013d7ba4a4513e8904a
│  │  ├─ 82
│  │  │  └─ 533b1f063383e7fc4d60790a3a60c353dc1c6e
│  │  ├─ 84
│  │  │  └─ 80e5a28aceb1aabdb94222ba17302748050c8a
│  │  ├─ 85
│  │  │  ├─ 3b1210c8999cb8fcedde66cb32d26659dc85dd
│  │  │  └─ 5e78d7e082e0da2f705901bca76811ad982bd1
│  │  ├─ 88
│  │  │  ├─ 98d9c1160ffb7dff69e11721092a175a0ced59
│  │  │  ├─ d510b81074d5170a9fc2356ff5477ebc66ee32
│  │  │  └─ fef071292b2293d097bfb696ac7417d635a42b
│  │  ├─ 89
│  │  │  └─ 7a603ce11644ac413ef2142376d2e44ec55094
│  │  ├─ 8c
│  │  │  └─ 7f935cc98a213f8f926b408d9f50adf24557e6
│  │  ├─ 8d
│  │  │  └─ edb2ada3bd78583129a008a8fef9b5469352b5
│  │  ├─ 90
│  │  │  └─ 26c84b7bf1bd4b91035553a49856b49b0800a9
│  │  ├─ 93
│  │  │  └─ 5566655c3f8aed49a070e67ab46b022e120695
│  │  ├─ 95
│  │  │  ├─ 1d438eac3ed3c5a4f88db337a874ff083d7d6c
│  │  │  └─ d22feba91fce61fc6d313cb504e05228490058
│  │  ├─ 97
│  │  │  └─ f828a53e3aeba877f035c79c3f14122a9ccdbb
│  │  ├─ 98
│  │  │  └─ a03487d8180063dffb959f9c41500726108323
│  │  ├─ 99
│  │  │  └─ ba44dc2227b4c14b3f0b3f85fa6edf93589ac5
│  │  ├─ 9a
│  │  │  └─ da2c654196008531afe7d28de1c65b7e6e812c
│  │  ├─ 9b
│  │  │  └─ 7ec8c9f0caf0ce57fb98744dc9a5c048f16aad
│  │  ├─ 9c
│  │  │  └─ 5de4aebcf057c7f837b67c9a2754f04935d084
│  │  ├─ 9e
│  │  │  └─ 7d2cefeb3837e08fc4568f6df0398216844d66
│  │  ├─ a1
│  │  │  ├─ 7f7f619fa1d253b4c28ebb908392c50c2461d3
│  │  │  └─ eb7feeb88e74e3c279a1e5f44e96ad829dc65f
│  │  ├─ a2
│  │  │  └─ ab6db6da81cbfe65bf4ee8a30d76af66982c43
│  │  ├─ a3
│  │  │  ├─ 20e2f86bbc48945c1a89bd643160193c440d46
│  │  │  ├─ 87bf9cd868fc96a4239b3a0125384d45b090ef
│  │  │  └─ 94be5773e29c5776c8383cfc4735db318e69d8
│  │  ├─ a8
│  │  │  ├─ 24292fe296827f6ae603f3bb4da08291b33092
│  │  │  └─ 307678663289100dd1820bf2b141117ab3b5e3
│  │  ├─ a9
│  │  │  └─ d7ec4bd2cc1d7cbddc1f840158b34685e23f28
│  │  ├─ ac
│  │  │  └─ 3893f1e7d588346cc6de14be21ff471f860c4c
│  │  ├─ ae
│  │  │  └─ 1b8be0cdaf37ad38c013fe256f33b3043bd4b1
│  │  ├─ af
│  │  │  └─ 4b33d3c046123344537e1366c4bec82b0a07c8
│  │  ├─ b2
│  │  │  ├─ 23db7d8fdacc3a11d55aaa232da85eb3fde460
│  │  │  └─ abec4bf11f0f4e5f51b80d74e0b668f159470a
│  │  ├─ b3
│  │  │  └─ 5bf3c6124506c5519b74dd122f3b946362c3a8
│  │  ├─ b7
│  │  │  └─ ff7623f9206632764822b8c37f1ec21d3e2a42
│  │  ├─ b8
│  │  │  └─ 753290098d6d519b460122c108c433148e7295
│  │  ├─ b9
│  │  │  └─ 7f8c38f74d5aba43d1e4a69e1dbadd834394b6
│  │  ├─ bb
│  │  │  ├─ 0233db288e1dcb3d006ca2b912feef65e28403
│  │  │  └─ 331670fe5e3143e5406685def7ddaca63334a6
│  │  ├─ bf
│  │  │  ├─ 6493e42636925610685e9de4b243c143915ea3
│  │  │  └─ ac06d6a70a5113093ec290e2b9dfa88012c71e
│  │  ├─ c1
│  │  │  └─ c55bb5603ecfe87e4c47d1ffaa1f759ee57d14
│  │  ├─ c2
│  │  │  ├─ 63d1284836a64be2589c7b167c4b0793ca12ca
│  │  │  └─ 9a9546e12fedb4157ad0be96d26f6a580dbc7d
│  │  ├─ c4
│  │  │  ├─ 5638a6b812c207c7094e46d65bf3fa671c8464
│  │  │  └─ ab246606c852d5f98527a8e1f5d67e9d244bb6
│  │  ├─ ca
│  │  │  └─ 6a99078644196d2bfbf00b595de42b25c93ef1
│  │  ├─ cb
│  │  │  └─ a4b017aad8536fd55e8df53d0fe7d73a571a46
│  │  ├─ cc
│  │  │  └─ f524c87487af663fb1107146c0a4a03b78af8b
│  │  ├─ cf
│  │  │  └─ c9451e571e236da29eea6154c2aae23569042e
│  │  ├─ d3
│  │  │  └─ 0f0939e005424325087619e1be1bc4fd889bee
│  │  ├─ d4
│  │  │  ├─ 78c9b27978ff295d9c1fc2b8d30a405f86ff0d
│  │  │  └─ f77a2a394f3c98be8512675aed95e72c3b0fd8
│  │  ├─ d7
│  │  │  └─ cb9de1cd534d6b2e869e43234f5639a45dec0c
│  │  ├─ da
│  │  │  └─ 5804bd746de4edd969777656cc76f25247f2b9
│  │  ├─ db
│  │  │  ├─ 79e9b727df3ad48effd2f748b6c5ad8e111a34
│  │  │  └─ bf99d2bac7b317bec5c6773b3dc9038b0ce08c
│  │  ├─ dd
│  │  │  └─ 6c29948170a5a24588ebad271e4841973b46b8
│  │  ├─ df
│  │  │  ├─ 78b9996bea69176216a4020ce8bc9d65277afa
│  │  │  └─ 849ac5d9a9ab14587fee37f6d1f6da8500260b
│  │  ├─ e2
│  │  │  └─ 195ab1b9418b6ba45795fb628936cb51434ca4
│  │  ├─ e6
│  │  │  └─ fc1314c5c0eae695dc535927db4f6594829d79
│  │  ├─ e8
│  │  │  └─ db3be889e7416485766d34453665c97fcb5cf8
│  │  ├─ e9
│  │  │  └─ 4463abe508bd2073fa34f56b1eae1b1a0fbbb4
│  │  ├─ eb
│  │  │  └─ 24da886334937434e8414e0f11e242554d2d43
│  │  ├─ ed
│  │  │  └─ 9dc416c1435fceca101e45fd8850ba8f92a10f
│  │  ├─ f1
│  │  │  ├─ 070ff179fa48aaed60970a242ef0a5d3f2f142
│  │  │  ├─ bf59b7823d51ae7be9ee0df1698a04368a2d8c
│  │  │  └─ e213d25e9288e7cb7da7b7d8a3a510042a6c46
│  │  ├─ f2
│  │  │  └─ 32500c3a516d5f2b346f5988d627d2b82886cd
│  │  ├─ f7
│  │  │  └─ a762b13fb78c496053e51c37ef317b4fa46b9f
│  │  ├─ fb
│  │  │  └─ e3a83f4adc07e83217e34b81c9455c6f727aa5
│  │  ├─ fc
│  │  │  └─ ce69d4070094a42499d6e616489f605a92be8b
│  │  ├─ fd
│  │  │  └─ d0a5cb79addc87d348bee7821182025ab51154
│  │  ├─ ff
│  │  │  └─ e8fc92875ad3bc29ad00af14f23d207b57c7db
│  │  ├─ info
│  │  └─ pack
│  │     ├─ pack-dd20f8c419cc926ef76effdbf4e610690159eac3.idx
│  │     └─ pack-dd20f8c419cc926ef76effdbf4e610690159eac3.pack
│  ├─ packed-refs
│  └─ refs
│     ├─ heads
│     │  ├─ master
│     │  └─ pratiksha-dev
│     ├─ remotes
│     │  └─ origin
│     │     ├─ HEAD
│     │     ├─ master
│     │     └─ pratiksha-dev
│     └─ tags
├─ .gitignore
├─ .vscode
│  └─ settings.json
├─ README.md
├─ analyses
│  └─ .gitkeep
├─ dbt_project.yml
├─ macros
│  ├─ .gitkeep
│  └─ generate_schema_name.sql
├─ models
│  ├─ intermediate
│  │  ├─ avni_gdgsgom_2023
│  │  │  ├─ approval_statuses_gdgs_2023.sql
│  │  │  ├─ cleaned
│  │  │  │  ├─ address_gdgs_23.sql
│  │  │  │  ├─ farmer_gdgs_23.sql
│  │  │  │  ├─ lok_sahbag_gdgs_2023.sql
│  │  │  │  ├─ machine_gdgs_23.sql
│  │  │  │  └─ work_order_gdgs_23.sql
│  │  │  ├─ encounters_gdgs_2023.sql
│  │  │  ├─ subjects_gdgs_2023.sql
│  │  │  └─ work_order_gdgs_2023.sql
│  │  ├─ avni_gdgsgom_2024
│  │  │  ├─ approval_statuses_gdgs_24.sql
│  │  │  ├─ cleaned
│  │  │  │  ├─ address_gdgs_2024.sql
│  │  │  │  ├─ farmer_gdgs_2024.sql
│  │  │  │  ├─ lok_sahbag_gdgs_2024.sql
│  │  │  │  ├─ machine_gdgs_2024.sql
│  │  │  │  └─ work_order_gdgs_2024.sql
│  │  │  ├─ encounters_2024.sql
│  │  │  ├─ subjects_2024.sql
│  │  │  └─ work_order_2024.sql
│  │  ├─ avni_niti_2022
│  │  │  ├─ approval_statuses_niti_2022.sql
│  │  │  ├─ cleaned
│  │  │  │  ├─ address_niti_22.sql
│  │  │  │  ├─ farmer_niti_22.sql
│  │  │  │  ├─ machine_niti_22.sql
│  │  │  │  └─ work_order_niti_22.sql
│  │  │  ├─ encounter_2022.sql
│  │  │  ├─ subjects_2022.sql
│  │  │  └─ work_order_2022.sql
│  │  ├─ avni_niti_2023
│  │  │  ├─ approval_statuses_niti_2023.sql
│  │  │  ├─ cleaned
│  │  │  │  ├─ address_niti_2023.sql
│  │  │  │  ├─ farmer_niti_2023.sql
│  │  │  │  ├─ machine_niti_2023.sql
│  │  │  │  └─ work_order_niti_2023.sql
│  │  │  ├─ encounter_2023.sql
│  │  │  ├─ subjects_2023.sql
│  │  │  └─ work_order_2023.sql
│  │  ├─ avni_niti_2024
│  │  │  ├─ approval_status_niti_2024.sql
│  │  │  ├─ cleaned
│  │  │  │  ├─ address_niti_2024.sql
│  │  │  │  ├─ farmer_niti_2024.sql
│  │  │  │  ├─ machine_niti_2024.sql
│  │  │  │  └─ work_order_niti_2024.sql
│  │  │  └─ encounters_niti_2024.sql
│  │  ├─ gramin_niti
│  │  │  ├─ approval_status_gramin.sql
│  │  │  ├─ cleaned
│  │  │  │  ├─ address_gramin.sql
│  │  │  │  ├─ farmer_gramin.sql
│  │  │  │  ├─ machine_gramin.sql
│  │  │  │  └─ work_order_gramin.sql
│  │  │  └─ encounters_gramin.sql
│  │  └─ intermediate_test.yml
│  ├─ prod
│  │  ├─ aggregated
│  │  │  ├─ gdgs_2023
│  │  │  │  ├─ farmer_agg_gdgs_23.sql
│  │  │  │  ├─ farmer_calc_silt_gdgs_23.sql
│  │  │  │  ├─ machine_gdgs_agg_23.sql
│  │  │  │  └─ progress_waterbodies_gdgs_23.sql
│  │  │  ├─ gdgs_2024
│  │  │  │  ├─ farmer_agg_gdgs_24.sql
│  │  │  │  ├─ farmer_calc_silt_gdgs_24.sql
│  │  │  │  ├─ machine_gdgs_agg_24.sql
│  │  │  │  └─ progress_waterbodies_gdgs_24.sql
│  │  │  ├─ gdgs_aggregated_2023_test.yml
│  │  │  ├─ gdgs_aggregated_2024_test.yml
│  │  │  ├─ gramin
│  │  │  │  ├─ farmer_agg_gramin.sql
│  │  │  │  ├─ farmer_calc_silt_gramin.sql
│  │  │  │  ├─ machine_gramin_aggregated.sql
│  │  │  │  └─ progress_waterbodies_gramin.sql
│  │  │  ├─ gramin_test.yml
│  │  │  ├─ niti_2022
│  │  │  │  ├─ farmer_agg_niti_22.sql
│  │  │  │  ├─ farmer_calc_silt_niti_22.sql
│  │  │  │  ├─ machine_niti_22_agg.sql
│  │  │  │  └─ progress_waterbodies_niti_22.sql
│  │  │  ├─ niti_2022_test.yml
│  │  │  ├─ niti_2023
│  │  │  │  ├─ farmer_agg_niti_23.sql
│  │  │  │  ├─ farmer_silt_calc_niti_2023.sql
│  │  │  │  ├─ machine_niti_2023_agg.sql
│  │  │  │  └─ progress_waterbodies_niti_23.sql
│  │  │  ├─ niti_2024
│  │  │  │  ├─ farmer_agg_niti_2024.sql
│  │  │  │  ├─ farmer_calc_silt_niti_2024.sql
│  │  │  │  ├─ machine_niti_2024_agg.sql
│  │  │  │  └─ progress_waterbodies_niti_2024.sql
│  │  │  ├─ niti_aggregated_2023_test.yml
│  │  │  └─ niti_aggregated_2024_test.yml
│  │  └─ final
│  │     ├─ final_gdgs_2023_test.yml
│  │     ├─ final_gdgs_2024_test.yml
│  │     ├─ final_gdgs_union_test.yml
│  │     ├─ final_gramin_test.yml
│  │     ├─ final_niti_2022_test.yml
│  │     ├─ final_niti_2023_test.yml
│  │     ├─ final_niti_2024_test.yml
│  │     ├─ final_niti_union_test.yml
│  │     ├─ final_org_test.yml
│  │     ├─ gdgs_2023
│  │     │  ├─ farmer_endline_gdgs_2023.sql
│  │     │  ├─ farmer_gdgs_2023_percentage.sql
│  │     │  ├─ farmer_silt_vulnerable_gdgs_23.sql
│  │     │  ├─ machine_endline_gdgs_2023.sql
│  │     │  ├─ machine_gdgs_metric_23.sql
│  │     │  └─ work_order_metric_gdgs_23.sql
│  │     ├─ gdgs_2024
│  │     │  ├─ farmer_endline.sql
│  │     │  ├─ farmer_gdgs_2024_percentage.sql
│  │     │  ├─ farmer_silt_vulnerable_gdgs_24.sql
│  │     │  ├─ machine_endline.sql
│  │     │  ├─ machine_gdgs_metric_24.sql
│  │     │  └─ work_order_metric_gdgs_24.sql
│  │     ├─ gdgs_union
│  │     │  ├─ farmer_calc_silt_gdgs_union.sql
│  │     │  ├─ farmer_endline_gdgs_union.sql
│  │     │  ├─ farmer_gdgs_agg_union.sql
│  │     │  ├─ farmer_gdgs_un_percentage.sql
│  │     │  ├─ farmer_silt_vulnerable_gdgs_union.sql
│  │     │  ├─ lok_sahbag_gdgs_union.sql
│  │     │  ├─ machine_endline_gdgs_union.sql
│  │     │  ├─ machine_gdgs_union.sql
│  │     │  ├─ mb_recording_gdgs_union.sql
│  │     │  ├─ progress_waterbodies_gdgs_union.sql
│  │     │  ├─ work_order_gdgs_union.sql
│  │     │  └─ work_order_metric_gdgs_union.sql
│  │     ├─ gramin
│  │     │  ├─ farmer_endline_gramin.sql
│  │     │  ├─ farmer_gramin_percentage.sql
│  │     │  ├─ farmer_silt_vulnerable_gramin.sql
│  │     │  ├─ machine_endline_gramin.sql
│  │     │  ├─ machine_gramin_metric.sql
│  │     │  └─ work_order_silt_calc.sql
│  │     ├─ niti_2022
│  │     │  ├─ farmer_endline_niti_2022.sql
│  │     │  ├─ farmer_niti_2022_percentage.sql
│  │     │  ├─ farmer_silt_vulnerable_niti_22.sql
│  │     │  ├─ machine_endline_niti_2022.sql
│  │     │  ├─ machine_niti_metric_22.sql
│  │     │  └─ work_order_metric_niti_22.sql
│  │     ├─ niti_2023
│  │     │  ├─ farmer_endline_niti_2023.sql
│  │     │  ├─ farmer_niti_2023_percentage.sql
│  │     │  ├─ farmer_silt_vulnerable_niti_23.sql
│  │     │  ├─ gram_panchayat_niti_23.sql
│  │     │  ├─ machine_endline_niti_2023.sql
│  │     │  ├─ machine_niti_metric_23.sql
│  │     │  └─ work_order_metric_niti_23.sql
│  │     ├─ niti_2024
│  │     │  ├─ farmer_endline_niti_2024.sql
│  │     │  ├─ farmer_niti_2024_percentage.sql
│  │     │  ├─ farmer_silt_vulnerable_niti_24.sql
│  │     │  ├─ machine_endline_niti_2024.sql
│  │     │  ├─ machine_niti_metric_24.sql
│  │     │  └─ work_order_metric_niti_2024.sql
│  │     ├─ niti_union
│  │     │  ├─ farmer_calc_silt_niti_union.sql
│  │     │  ├─ farmer_endline_niti_union.sql
│  │     │  ├─ farmer_niti_agg_union.sql
│  │     │  ├─ farmer_niti_un_percentage.sql
│  │     │  ├─ farmer_silt_vulnerable_niti_union.sql
│  │     │  ├─ gram_panchayat_niti_union.sql
│  │     │  ├─ machine_endline_niti_union.sql
│  │     │  ├─ machine_niti_union.sql
│  │     │  ├─ mb_recording_niti_union.sql
│  │     │  ├─ progress_waterbodies_niti_union.sql
│  │     │  ├─ work_order_metric_niti_union.sql
│  │     │  └─ work_order_niti_union.sql
│  │     └─ org
│  │        ├─ farmer_agg_org.sql
│  │        ├─ farmer_calc_silt_org.sql
│  │        ├─ farmer_endline_org.sql
│  │        ├─ farmer_percentage_org.sql
│  │        ├─ farmer_silt_vulnerable_org.sql
│  │        ├─ gram_panchayat_niti_org.sql
│  │        ├─ lok_sahbag_org.sql
│  │        ├─ machine_endline_org.sql
│  │        ├─ machine_org.sql
│  │        ├─ progress_waterbodies_org.sql
│  │        └─ work_order_metric_org.sql
│  └─ source.yml
├─ package-lock.yml
├─ packages.yml
├─ seeds
│  └─ .gitkeep
├─ snapshots
│  └─ .gitkeep
└─ tests
   └─ .gitkeep

```
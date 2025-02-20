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

### Project Tree:
```
dbt_atcef
├─ .gitignore
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
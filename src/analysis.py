from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, countDistinct, desc, dense_rank, row_number,when
from pyspark.sql.window import Window
import logging

class AnalysisRunner:
    def __init__(self, spark: SparkSession, data: dict, output_path: str):
        self.spark = spark
        self.data = data
        self.output_path = output_path
        self.logger = logging.getLogger(__name__)
        self.logger.debug("Initializing AnalysisRunner") # Debug log

    def _save_output(self, df: DataFrame, analysis_name: str) -> None:
        """Save analysis result to CSV"""
        self.logger.debug(f"Saving {analysis_name} results")  # Debug log
        output_dir = f"{self.output_path}/{analysis_name}"
        df.write.csv(output_dir, header=True, mode="overwrite")
        

    def run_analysis(self, analysis_name: str) -> DataFrame:
        """Execute specified analysis"""
        analysis_method = getattr(self, analysis_name, None)
        if not analysis_method:
            raise ValueError(f"Analysis {analysis_name} not found")
        return analysis_method()

    def analysis_1(self) -> DataFrame:
        """
        Analytics 1: Find the number of crashes (accidents) in which number of males killed are greater than 2?
        - Filter male casualties (DEATH_CNT=1 per person)
        - Group by crash, count males killed, filter >2
        """
        df = self.data['primary_person'].filter(
            (col('PRSN_GNDR_ID') == 'MALE') & (col('DEATH_CNT') == 1) # Each row represents 1 death
        ).groupBy('CRASH_ID').agg(
            count("DEATH_CNT").alias("NO_OF_MALES_KILLED") # Count males per crash
        ).filter(col("NO_OF_MALES_KILLED") > 2) # Filter crashes with >2 fatalities
        
        print(f"\nAnalysis 1: the number of crashes (accidents) in which number of males killed are greater than 2: {df.count()}")
        self._save_output(df, "Analysis1")
        return df

    def analysis_2(self) -> DataFrame:
        """
        Analysis 2: How many two wheelers are booked for crashes? 
        - Filter motorcycle body styles
        - Count distinct VINs to avoid duplicates
        """
        df = self.data['units'].filter(
            col('VEH_BODY_STYL_ID').isin(['POLICE MOTORCYCLE', 'MOTORCYCLE']) # Identify two-wheelers
        ).select('VIN').dropna() # VIN uniquely identifies a vehicle
        
        count_val = df.distinct().count() # Ensure no duplicate vehicles
        print(f"\nAnalysis 2: Two-wheelers booked for crashes: {count_val}")
        self._save_output(df, "Analysis2")
        return df

    def analysis_3(self) -> DataFrame:
        """
        Analysis 3: Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.
        - Join driver (PRSN_TYPE_ID=DRIVER) and vehicle data
        - Filter: Airbag not deployed, driver died, car body style
        - Group by make, count vehicles, take top 5
        """
        df = self.data['primary_person'].alias("pp").join(
            self.data['units'], ['CRASH_ID', 'UNIT_NBR'], 'inner'
        ).filter(
            (col('PRSN_TYPE_ID').contains('DRIVER')) & # Ensure driver is present
            (col('PRSN_AIRBAG_ID') == 'NOT DEPLOYED') & # Airbag failure
            (col('pp.DEATH_CNT') == 1) &
            (col('VEH_BODY_STYL_ID').contains('CAR')) # Restrict to cars
        ).groupBy('VEH_MAKE_ID').agg(
            countDistinct("VIN").alias('COUNT_VEHICLES') # Count unique cars per make
        ).orderBy(desc('COUNT_VEHICLES'), 'VEH_MAKE_ID').limit(5)
        
        print("\nAnalysis 3: the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy:")
        df.show(truncate=False)
        self._save_output(df, "Analysis3")
        return df

    def analysis_4(self) -> DataFrame:
        """
        Analysis 4: Determine number of Vehicles with driver having valid licences involved in hit and run? 
        - Join driver and vehicle data
        - Filter: Valid license classes, hit-and-run flag (VEH_HNR_FL=Y)
        """
        valid_licenses = ['CLASS A', 'CLASS B', 'CLASS C', 'CLASS M', 
                         'CLASS A AND M', 'CLASS B AND M', 'CLASS C AND M']
        df = self.data['primary_person'].join(
            self.data['units'], ['CRASH_ID', 'UNIT_NBR'], 'inner'
        ).filter(
            (col('PRSN_TYPE_ID').contains('DRIVER')) &
            (col('DRVR_LIC_CLS_ID').isin(valid_licenses)) & # Valid license check
            (col('VEH_HNR_FL') == 'Y') # Hit-and-run flag
        ).agg(countDistinct("VIN").alias("NO_OF_VEHICLES"))
        
        print("\nAnalysis 4: Number of Vehicles with driver having valid licences involved in hit and run:")
        df.show(truncate=False)
        self._save_output(df, "Analysis4")
        return df

    def analysis_5(self) -> DataFrame:
        """
        Analysis 5: Which state has highest number of accidents in which females are not involved? 
        - Exclude FEMALE from primary_person
       - Join with units to get state data
       - Count crashes per state, take top 1
       """
        gender = ['MALE'] 
        df = self.data['units'].join(
            self.data['primary_person'].filter(col("PRSN_GNDR_ID").isin(gender)), 
            ["CRASH_ID"], 'inner'
        ).groupBy("VEH_LIC_STATE_ID").agg(
            countDistinct("CRASH_ID").alias("crash_count") # Count unique crashes
        ).orderBy(desc("crash_count")).limit(1)
        result = df.collect()[0]['VEH_LIC_STATE_ID']
        print(f"\nAnalysis 5: State with highest male-only accidents: {result}")
        self._save_output(df, "Analysis5")
        return df

    def analysis_6(self) -> DataFrame:
        """
        Analysis 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
        - Filter injury severity types
        - Join with vehicle data to get make
        - Rank by injury count, select ranks 3-5
        """
        injury_flags = ['KILLED', 'NON-INCAPACITATING INJURY', 
                       'POSSIBLE INJURY', 'INCAPACITATING INJURY']
        df = self.data['primary_person'].filter(
            col('PRSN_INJRY_SEV_ID').isin(injury_flags) # Include all injury types
        ).join(
            self.data['units'].select(['CRASH_ID', 'UNIT_NBR', 'VEH_MAKE_ID']),
            ['CRASH_ID', 'UNIT_NBR'], 'inner'
        ).filter(col('VEH_MAKE_ID') != 'NA').groupBy('VEH_MAKE_ID').agg(
            count('CRASH_ID').alias('count')
        ).withColumn(
            'dense_rank', dense_rank().over(Window.orderBy(desc('count'))) # Strict ranking
        ).filter((col('dense_rank') >= 3) & (col('dense_rank') <= 5)).select('VEH_MAKE_ID') # 3rd-5th ranks
        
        print("\nAnalysis 6: The Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death:")
        df.show(truncate=False)
        self._save_output(df, "Analysis6")
        return df

    def analysis_7(self) -> DataFrame:
        """
        Analysis 7: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
        - Outer join to retain all vehicles
        - Group by body style and ethnicity, rank within groups
        - Select top ethnicity per body style
        """
        person_units_join_df = self.data['primary_person'].join(
            self.data['units'], ['CRASH_ID', 'UNIT_NBR'], 'outer' # Include all vehicles
        )
        df = person_units_join_df.filter(
            (col('VEH_BODY_STYL_ID') != 'NA') & 
            (col('VEH_BODY_STYL_ID') != 'UNKNOWN') # Clean data
        ).groupBy(['VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID']).agg(
            count('CRASH_ID').alias('crash_count')
        ).withColumn(
            'dense_rank', dense_rank().over(
                Window.partitionBy('VEH_BODY_STYL_ID').orderBy(desc('crash_count'))
            )
        ).filter(col('dense_rank') == 1).select(['VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID']) # Top ethnicity per body style
        
        print("\nAnalysis 7: Top ethnic group per vehicle body style:")
        df.show(truncate=False)
        self._save_output(df, "Analysis7")
        return df

    def analysis_8(self) -> DataFrame:
        """
        Analysis 8: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
        - Filter alcohol-positive results and valid zip codes
       - Restrict to passenger cars
       - Count crashes per zip, take top 5
       """
        car_types = ['PASSENGER CAR, 2-DOOR', 'PASSENGER CAR, 4-DOOR', 'POLICE CAR/TRUCK',
                    'SPORT UTILITY VEHICLE', 'NEV-NEIGHBORHOOD ELECTRIC VEHICLE', 'VAN']
        person_units_join_df = self.data['primary_person'].join(
            self.data['units'], ['CRASH_ID', 'UNIT_NBR'], 'outer'
        )
        df = person_units_join_df.filter(
            (col('PRSN_ALC_RSLT_ID') == 'Positive') &  # Alcohol involved
            (col('DRVR_ZIP').isNotNull()) & # Valid zip
            (col('VEH_BODY_STYL_ID').isin(car_types)) # Passenger cars only
        ).groupBy('DRVR_ZIP').agg( 
            countDistinct('CRASH_ID').alias('crash_count') # Unique crashes per zip
        ).orderBy(desc('crash_count')).limit(5)
        
        print("\nAnalysis 8: Top 5 zip codes with alcohol-related crashes:")
        df.show(truncate=False)
        self._save_output(df, "Analysis8")
        return df

    def analysis_9(self) -> DataFrame:
        """
        Analysis 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
        - Filter vehicles with damage scale >4
        - Join with damages where NO DAMAGE to property
        - Check for valid insurance (FIN_RESP_TYPE_ID ≠ NA)
        """
        damage_flags = ['DAMAGED 5', 'DAMAGED 6', 'DAMAGED 7 HIGHEST'] # Assumed to represent damage levels >4
        car_types = ['PASSENGER CAR, 2-DOOR', 'PASSENGER CAR, 4-DOOR', 'POLICE CAR/TRUCK', 'PICKUP',
                    'SPORT UTILITY VEHICLE', 'NEV-NEIGHBORHOOD ELECTRIC VEHICLE', 'VAN']
        
        damaged_units = self.data['units'].filter(
            (col('VEH_DMAG_SCL_1_ID').isin(damage_flags)) | 
            (col('VEH_DMAG_SCL_2_ID').isin(damage_flags)) # High damage on either scale
        )
        damages_join = damaged_units.join(
            self.data['damages'].filter(col('DAMAGED_PROPERTY').contains('NO DAMAGE')), # No property damage
            ['CRASH_ID'], 'inner'
        )
        df = damages_join.filter(
            (~col('FIN_RESP_TYPE_ID').isin(['NA'])) & # Insurance exists
            (col('VEH_BODY_STYL_ID').isin(car_types)) # Passenger cars
        ).select('CRASH_ID').distinct()   # Unique crash IDs
        
        print(f"\nAnalysis 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level is above 4 and car avails Insurance: {df.count()}")
        self._save_output(df, "Analysis9")
        return df

    def analysis_10(self) -> DataFrame:
        """
        Analysis 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)
        - Identify top 25 states and top 10 colors from data
        - Filter speeding charges (CHARGE contains 'SPEED')
        - Join with valid licenses, top states, and top colors
        - Aggregate offences per make
        """
        top_25_states = self.data['units'].groupBy("VEH_LIC_STATE_ID").agg(
            count("*").alias("offense_count")
        ).orderBy(desc("offense_count")).limit(25)
        
        top_10_colors = self.data['units'].groupBy("VEH_COLOR_ID").agg(
            count("*").alias("color_count")
        ).orderBy(desc("color_count")).limit(10)
        
        df = self.data['units'].join(
            self.data['charges'].filter(col("CHARGE").contains("SPEED")), # Speeding offences
            ["CRASH_ID", "UNIT_NBR"]
        ).join(
            self.data['primary_person'].filter(
                col("DRVR_LIC_TYPE_ID").isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]) # Valid license
            ), ["CRASH_ID", "UNIT_NBR"]
        ).join(
            top_25_states, "VEH_LIC_STATE_ID" # Filter to top 25 states
        ).join(
            top_10_colors, "VEH_COLOR_ID" # Filter to top 10 colors
        ).groupBy("VEH_MAKE_ID").agg(
            count("*").alias("total_offenses")  # Total offences per make
        ).orderBy(desc("total_offenses")).limit(5)
        
        print("\nAnalysis 10: Top 5 makes with speeding offences:")
        df.show(truncate=False)
        self._save_output(df, "Analysis10")
        return df
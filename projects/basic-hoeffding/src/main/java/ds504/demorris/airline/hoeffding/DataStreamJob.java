/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ds504.demorris.airline.hoeffding;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;


//These Flink packages are to import MOA components for online learning
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

//MOA packages
import moa.classifiers.Classifier;
import moa.classifiers.functions.NoChange;
import moa.classifiers.trees.HoeffdingTree;
import moa.core.InstanceExample;
import moa.core.FastVector;
import moa.streams.InstanceStream;
import moa.options.ClassOption;

//SAMOA packages
import com.yahoo.labs.samoa.instances.InstancesHeader;
import com.yahoo.labs.samoa.instances.Instance;
import com.yahoo.labs.samoa.instances.Instances;
import com.yahoo.labs.samoa.instances.Attribute;
import com.yahoo.labs.samoa.instances.DenseInstance;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {

	public static void main(String[] args) throws Exception {
			// Sets up the execution environment, which is the main entry point
			// to building Flink applications.
			final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

			String[] classNames = {"No Delay","Delay"};

			DataStream<InstanceExample> train = env
					.readTextFile("file:///root/data/airline/airlines_train_regression_nohead.csv").setParallelism(1)
					.map(new convertToInstance(780,classNames,0)).setParallelism(1);

			SingleOutputStreamOperator<Classifier> classifier = train.process(new ModelProcessFunction("HoeffdingTree -c 0.02",HoeffdingTree.class, 1000,0.5));

			// Execute program, beginning computation.
			env.execute("Airline Base");
	}

    public static class convertToInstance implements MapFunction<String, InstanceExample> {

		protected InstancesHeader streamHeader;

		protected int numAttributes;
		protected int numClasses;
		protected String[] classNames;
		protected int targetIndex;
		protected int delayThreshold=15; // The FAA gives flights a 15 minute grace window from the departure time before considering a flight to be delayed

		public convertToInstance(int numAttribues, String[] classNames, int targetIndex){
			this.numAttributes = numAttribues;
			this.numClasses = classNames.length;
			this.classNames = classNames;
			this.targetIndex = targetIndex;
			generateHeader();
		}

        private Double targetLogic(String[] splitText){
            double target = 1;
			if (Integer.parseInt(splitText[targetIndex]) <= delayThreshold) {
				target = 0;
			}
                        return target; 
		}

		private Double[] oneHotDayOfWeek(String DoW){
			Double[] DayOfWeek = {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
			DayOfWeek[(int)Double.parseDouble(DoW)-1] = 1.0;
			return DayOfWeek;
		}

		private Double[] oneHotCarrier(String carrier){
			String[] strCarriers = {"9E","AA","AS","B6","CO","DH","DL","EA","EV","F9","FL","HA","HP","KH",
				"ML (1)","MQ","NW","OH","OO","PA (1)","PI","PS","TW","TZ","UA","US","VX","WN","XE","YV"};
			Double[] dblCarriers = new Double[strCarriers.length];
			
			for (int i=0; i<strCarriers.length; i++){
				if(strCarriers[i].equals(carrier)){
					dblCarriers[i] = 1.0;
				} else {
					dblCarriers[i] = 0.0;
				}
			}
			return dblCarriers;
		}

		private Double[] oneHotOrigin(String origin){
			String[] strOrigins = {"ABE","ABI","ABQ","ABR","ABY","ACK","ACT","ACV","ACY","ADK","ADQ","AEX","AGS",
				"AKN","ALB","ALO","AMA","ANC","ANI","APF","APN","ART","ASE","ATL","ATW","AUS","AVL","AVP","AZA",
				"AZO","BDL","BET","BFL","BGM","BGR","BHM","BIL","BIS","BJI","BKG","BLI","BMI","BNA","BOI","BOS",
				"BPT","BQK","BQN","BRD","BRO","BRW","BTM","BTR","BTV","BUF","BUR","BWI","BZN","CAE","CAK","CCR",
				"CDC","CDV","CEC","CHA","CHO","CHS","CIC","CID","CIU","CLD","CLE","CLL","CLT","CMH","CMI","CMX",
				"COD","COS","COU","CPR","CRP","CRW","CSG","CVG","CWA","CYS","DAB","DAL","DAY","DBQ","DCA","DEN",
				"DET","DFW","DHN","DIK","DLG","DLH","DRO","DRT","DSM","DTW","DUT","EAU","ECP","EFD","EGE","EKO",
				"ELM","ELP","ERI","ESC","EUG","EVV","EWN","EWR","EYW","FAI","FAR","FAT","FAY","FCA","FLG","FLL",
				"FLO","FNT","FOE","FSD","FSM","FWA","GCC","GCK","GCN","GEG","GFK","GGG","GJT","GNV","GPT","GRB",
				"GRI","GRK","GRR","GSO","GSP","GST","GTF","GTR","GUC","GUM","HDN","HHH","HIB","HKY","HLN","HNL",
				"HOB","HOU","HPN","HRL","HSV","HTS","HVN","IAD","IAH","ICT","IDA","ILE","ILG","ILM","IMT","IND",
				"INL","IPL","ISN","ISO","ISP","ITH","ITO","IYK","JAC","JAN","JAX","JFK","JLN","JNU","KOA","KSM",
				"KTN","LAN","LAR","LAS","LAW","LAX","LBB","LCH","LEX","LFT","LGA","LGB","LIH","LIT","LMT","LNK",
				"LNY","LRD","LSE","LWB","LWS","LYH","MAF","MAZ","MBS","MCI","MCN","MCO","MDT","MDW","MEI","MEM",
				"MFE","MFR","MGM","MHK","MHT","MIA","MIB","MKC","MKE","MKG","MKK","MLB","MLI","MLU","MMH","MOB",
				"MOD","MOT","MQT","MRY","MSN","MSO","MSP","MSY","MTH","MTJ","MVY","MWH","MYR","OAJ","OAK","OGD",
				"OGG","OKC","OMA","OME","ONT","ORD","ORF","ORH","OTH","OTZ","OXR","PAH","PBI","PDX","PFN","PHF",
				"PHL","PHX","PIA","PIE","PIH","PIT","PLN","PMD","PNS","PPG","PSC","PSE","PSG","PSP","PUB","PVD",
				"PWM","RAP","RDD","RDM","RDR","RDU","RFD","RHI","RIC","RKS","RNO","ROA","ROC","ROP","ROR","ROW",
				"RST","RSW","SAF","SAN","SAT","SAV","SBA","SBN","SBP","SCC","SCE","SCK","SDF","SEA","SFO","SGF",
				"SGU","SHD","SHV","SIT","SJC","SJT","SJU","SLC","SLE","SMF","SMX","SNA","SOP","SPI","SPN","SPS",
				"SRQ","STL","STT","STX","SUN","SUX","SWF","SYR","TEX","TLH","TOL","TPA","TRI","TTN","TUL","TUP",
				"TUS","TVC","TVL","TWF","TXK","TYR","TYS","UCA","UTM","VCT","VIS","VLD","VPS","WRG","WYS","XNA",
				"YAK","YAP","YKM","YUM"};
			Double[] dblOrigin = new Double[strOrigins.length];
			for(int i=0; i<strOrigins.length; i++){
				if(strOrigins[i].equals(origin)){
					dblOrigin[i] = 1.0;
				} else {
					dblOrigin[i] = 0.0;
				}
			}
			return dblOrigin;

		}

		private Double[] oneHotDestination(String destination){
			String[] strDests = {"ABE","ABI","ABQ","ABR","ABY","ACK","ACT","ACV","ACY","ADK","ADQ","AEX","AGS","AKN",
				"ALB","ALO","AMA","ANC","ANI","APF","APN","ART","ASE","ATL","ATW","AUS","AVL","AVP","AZA","AZO","BDL",
				"BET","BFL","BGM","BGR","BHM","BIL","BIS","BJI","BKG","BLI","BMI","BNA","BOI","BOS","BPT","BQK","BQN",
				"BRD","BRO","BRW","BTM","BTR","BTV","BUF","BUR","BWI","BZN","CAE","CAK","CCR","CDC","CDV","CEC","CHA",
				"CHO","CHS","CIC","CID","CIU","CLD","CLE","CLL","CLT","CMH","CMI","CMX","COD","COS","COU","CPR","CRP",
				"CRW","CSG","CVG","CWA","CYS","DAB","DAL","DAY","DBQ","DCA","DEN","DET","DFW","DHN","DIK","DLG","DLH",
				"DRO","DRT","DSM","DTW","DUT","EAU","ECP","EFD","EGE","EKO","ELM","ELP","ERI","ESC","EUG","EVV","EWN",
				"EWR","EYW","FAI","FAR","FAT","FAY","FCA","FLG","FLL","FLO","FNT","FOE","FSD","FSM","FWA","GCC","GCK",
				"GCN","GEG","GFK","GGG","GJT","GNV","GPT","GRB","GRI","GRK","GRR","GSO","GSP","GST","GTF","GTR","GUC",
				"GUM","HDN","HHH","HIB","HKY","HLN","HNL","HOB","HOU","HPN","HRL","HSV","HTS","HVN","IAD","IAH","ICT",
				"IDA","ILE","ILG","ILM","IMT","IND","INL","IPL","ISN","ISO","ISP","ITH","ITO","IYK","JAC","JAN","JAX",
				"JFK","JLN","JNU","KOA","KSM","KTN","LAN","LAR","LAS","LAW","LAX","LBB","LCH","LEX","LFT","LGA","LGB",
				"LIH","LIT","LMT","LNK","LNY","LRD","LSE","LWB","LWS","LYH","MAF","MAZ","MBS","MCI","MCN","MCO","MDT",
				"MDW","MEI","MEM","MFE","MFR","MGM","MHK","MHT","MIA","MIB","MKE","MKG","MKK","MLB","MLI","MLU","MMH",
				"MOB","MOD","MOT","MQT","MRY","MSN","MSO","MSP","MSY","MTH","MTJ","MVY","MWH","MYR","OAJ","OAK","OGG",
				"OKC","OMA","OME","ONT","ORD","ORF","ORH","OTH","OTZ","OXR","PAH","PBI","PDX","PFN","PHF","PHL","PHX",
				"PIA","PIE","PIH","PIT","PLN","PMD","PNS","PPG","PSC","PSE","PSG","PSP","PUB","PVD","PVU","PWM","RAP",
				"RDD","RDM","RDR","RDU","RFD","RHI","RIC","RKS","RNO","ROA","ROC","ROP","ROR","ROW","RST","RSW","SAF",
				"SAN","SAT","SAV","SBA","SBN","SBP","SCC","SCE","SCK","SDF","SEA","SFO","SGF","SGU","SHD","SHV","SIT",
				"SJC","SJT","SJU","SLC","SLE","SMF","SMX","SNA","SOP","SPI","SPN","SPS","SRQ","STL","STT","STX","SUN",
				"SUX","SWF","SYR","TEX","TLH","TOL","TPA","TRI","TTN","TUL","TUP","TUS","TVC","TVL","TWF","TXK","TYR",
				"TYS","UCA","UTM","VCT","VIS","VLD","VPS","WRG","WYS","XNA","YAK","YAP","YKM","YUM"};
			Double[] dblDest = new Double[strDests.length];
			for(int i=0; i<strDests.length; i++){
				if(strDests[i].equals(destination)){
					dblDest[i] = 1.0;
				} else {
					dblDest[i] = 0.0;
				}
			}
			return dblDest;

		}

		private Double[] featureLogic(String feature, int featureIndex){
			if(featureIndex == 3){
				return oneHotDayOfWeek(feature);
			} else if(featureIndex == 6){
				return oneHotCarrier(feature.replace("'",""));
			} else if(featureIndex == 7){
				return oneHotOrigin(feature.replace("'",""));
			} else if(featureIndex == 8){
				return oneHotDestination(feature.replace("'",""));
			} else {
				return new Double[]{Double.parseDouble(feature)};

			}
		}

		@Override
		public InstanceExample map(String inputText) throws Exception{
			String[] splitText = inputText.split(",");

			double target = targetLogic(splitText);

			double[] features = new double[numAttributes+1];
			int featureIndex = 0;
			for (int i = 0; i < splitText.length; i++){
				if (i != targetIndex){
					Double[] extractedFeatures = featureLogic(splitText[i], i);
					for (int j=0; j < extractedFeatures.length; j++){
						features[featureIndex++] = extractedFeatures[j];
					}
				}
			}
			
			Instance inst = new DenseInstance(1.0,features);
			inst.setDataset(getHeader());
			inst.setClassValue(target);
			return new InstanceExample(inst);
		}

		protected void generateHeader(){
			FastVector attributes = new FastVector();
			for (int i=0; i < this.numAttributes; i++){
				attributes.addElement(new Attribute("att"+(i+1)));
			}

			FastVector classLabels = new FastVector();
                        for (String className: classNames){
                                classLabels.addElement(className);
                        }
			attributes.addElement(new Attribute("class",classLabels));

			this.streamHeader = new InstancesHeader(new Instances("DATA",attributes,0));
			this.streamHeader.setClassIndex(this.streamHeader.numAttributes()-1);
		}

		public InstancesHeader getHeader(){
			return this.streamHeader;
		}
	}
}


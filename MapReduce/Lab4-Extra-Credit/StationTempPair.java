import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


public class StationTempPair  implements WritableComparable<StationTempPair> {
		private Text StationId;
		private FloatWritable Temperature;
		
		public StationTempPair(){
			this.StationId = new Text();
			this.Temperature = new FloatWritable(0);
		}
		
		public void setTemperature(float temp){
			this.Temperature.set(temp);
		}
		
		public void setStationId(String stationId){
			this.StationId.set(stationId);
		}
		
		public String getStationId(){
			return this.StationId.toString();
		}
		
		public float getTemperature(){
			return this.Temperature.get();
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			StationId.readFields(in);
			Temperature.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			StationId.write(out); 
			Temperature.write(out);
		}
		
		@Override
		public String toString() {
			return "StationId " + this.getStationId() +" Temp is " + this.getTemperature();
		}

		@Override
		public int compareTo(StationTempPair o) {
			int stationSortReult =  this.getStationId().compareTo(o.getStationId());
			
            if(stationSortReult == 0)
            {
                if (this.getTemperature() > o.getTemperature()) { 
                    return 1;
                }
                
                if (this.getTemperature()  < o.getTemperature())
                {
                    return -1;
                }
                return 0;

            }
            return stationSortReult;
		}

	}
	
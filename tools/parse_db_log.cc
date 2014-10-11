#include <iostream>
#include <fstream>
#include <string>
#include <time.h>
#include <stdio.h>
#include <math.h>
#include <rocksdb/Slice.h>
#include <vector>
#include <sstream> 

namespace rocksdb
{
  enum EventLogType{
    kNoUseEvent,
    kCompactionEvent,
    kFlushEvent
  };

  class EventLog{
  private:
    time_t event_time_;
    
  public:
    EventLog(){};
    ~EventLog(){};
    void SetEventTime(time_t time){event_time_=time;};
    time_t GetEventTime(){return event_time_;};
    
    virtual bool Parse(std::string log_string)=0;

    int event_usec_;
  };
  
  class CompactionEvent : public EventLog
  {
  public:
    CompactionEvent():base_layer_(-1){};
    ~CompactionEvent(){};
    
    bool Parse(std::string log_string)
    {
      if (log_string.find("Origin") != std::string::npos){
	log_string = log_string.substr(log_string.find(")") + 1);
      }
      if (log_string.find("Compaction start") != std::string::npos){
	compaction_type_ = kCompactionCreate;
	log_string = log_string.substr(log_string.find(":") + 1);
	char file_input_ids_origin[1024], file_input_ids_high[1024];
	std::sscanf(log_string.c_str(), " Base version %d Base level %d, seek compaction:0, inputs: [%s], [%s]",
		    &base_version_id_, &base_layer_, file_input_ids_origin, file_input_ids_high);
	std::cout << "Compaction START  @"<< GetEventTime() <<": base_version_id : "<< base_version_id_ << " base_layer : " << base_layer_ << std::endl;
	return true;
      }else if(log_string.find("Compacted to") != std::string::npos){
	compaction_type_ = kCompactionFinish;
	log_string = log_string.substr(log_string.find(":") + 1);
	char other[256];
	std::sscanf(log_string.c_str(), " Base version %d files%s", &base_version_id_, other);  
	std::cout << "Compaction FINISH @"<< GetEventTime() <<": base_version_id : "<< base_version_id_  << std::endl;
	return true;
      };
      return false;
    }
    
    enum CompactionType{
      kCompactionCreate=0,
      kCompactionFinish=1
    };
    int base_version_id_;
    int base_layer_;
    CompactionType compaction_type_;
    
  private:
    //int target_base_files_num_;
    //int target_high_files_num_;
    std::vector<int> target_file_ids_base_;
    std::vector<int> target_file_ids_high_;
    
  };

  class FlushEvent : public EventLog
  {
  public:
    FlushEvent(){};
    ~FlushEvent(){};

    bool Parse(std::string log_string)
    {
      log_string = log_string.substr(log_string.find("]") + 2);
      if(log_string.find("Level-0 flush") != std::string::npos){
	if(log_string.find("started") != std::string::npos){
	  flush_type_ = kFlushCreate;
	  std::sscanf(log_string.c_str(), "Level-0 flush table #%d", &version_id_);
	  //std::cout << "Flush Start version_id : #" << version_id_  <<std::endl;
	  return true;
	}
	if(log_string.find("bytes") != std::string::npos){
	  flush_type_ = kFlushFinish;
	  //std::cout << log_string << std::endl;
	  std::sscanf(log_string.c_str(), " Level-0 flush table #%d: %ld bytes OK", &version_id_, &write_bytes_);
	  //std::cout << "Flush End version_id : #" << version_id_ << " : Bytes" << write_bytes_ <<std::endl;
	  return true;
	}
      }
      return false;
    };

    enum FlushType{
      kFlushCreate,
      kFlushFinish
    };
    int version_id_;
    FlushType flush_type_;

  private:
    long write_bytes_;
  };

};//namespace rocksdb


namespace rocksdb
{
  class LOGParser{
  private:
    std::vector<CompactionEvent*> parsed_compaction_event_logs_;
    std::vector<FlushEvent*> parsed_flush_event_logs_;

  public:
    std::vector<CompactionEvent*> getCompactionEvent(){return parsed_compaction_event_logs_;};
    std::vector<FlushEvent*> getFlushEvent(){return parsed_flush_event_logs_;};
    LOGParser(){};
    ~LOGParser(){};

    void Parse(std::string filename)
    {
      std::ifstream LOGfile(filename);
      std::string line;
      
      while (std::getline(LOGfile, line))
	{
	  struct tm t;
	  char thread_id[128];
	  int now_sec;
	  int ret = std::sscanf(line.c_str(),
				"%04d/%02d/%02d-%02d:%02d:%02d.%06d %s",
				&t.tm_year,
				&t.tm_mon,
				&t.tm_mday,
				&t.tm_hour,
				&t.tm_min,
				&t.tm_sec,
				&now_sec,
				thread_id
				);
	  if(ret == EOF)
	    std::cout << "ERROR" << std::endl;
	  else if(ret == 0){}
	  else{
	    t.tm_year -= 1900;
	    t.tm_mon  -= 1;
	    time_t unix_timestamp = mktime(&t);

	    std::cout << t.tm_year << "/" << t.tm_mon << "/" << t.tm_mday << " - " << t.tm_hour << ":" << t.tm_min << ":" << t.tm_sec << std::endl;

	    std::string thread_string(thread_id);
	    int cutoff_offset = 28 + thread_string.length();
	    std::string log_string = line.substr(cutoff_offset).c_str();
	    EventLogType log_type = GetTypeOfToken(log_string);
	    if ( log_type == kCompactionEvent){
	      CompactionEvent* event_log = new CompactionEvent();
	      event_log->SetEventTime(unix_timestamp);
	      event_log->event_usec_ = now_sec;
	      if(event_log->Parse(log_string))
		parsed_compaction_event_logs_.push_back(event_log);
	    }else if( log_type == kFlushEvent ){
	      FlushEvent* event_log = new FlushEvent();
	      event_log->SetEventTime(unix_timestamp);
	      event_log->event_usec_ = now_sec;
	      if(event_log->Parse(log_string))
		parsed_flush_event_logs_.push_back(event_log);
	    }else if( log_type == kNoUseEvent ){
	    }
	  }
	}
    };

    EventLogType GetTypeOfToken(std::string token){
      if (token.find("[default]") != std::string::npos){
	if (token.find("Compact") != std::string::npos)
	  return kCompactionEvent;
	//This line will be column family information
	else if(token.find("Level-0") != std::string::npos)
	  return kFlushEvent;
      }
      return kNoUseEvent;
    };
  };
};//namespace rocksdb


namespace rocksdb
{
  class VisualizationGenerator{
  public:
    VisualizationGenerator()
    {
      log_parser_.Parse("/tmp/rocksdbtest-501/dbbench/LOG");
    };
    ~VisualizationGenerator(){};

    void PrintEventDatas()
    {
      std::cout << "Print " << std::endl;

      std::vector<std::string> colors{
	std::string("#0066FF"),
	  std::string("#FF6600"),
	  std::string("#FF66FF"),
	  std::string("#990033"),
	  std::string("#00FF77"),
	  std::string("#66FF33"),
	  std::string("FFFF00")};
      std::ofstream variables_js("variables.js");
      variables_js << "var event_datas = " << std::endl;
      variables_js << "["                  << std::endl;
      std::cout << "Fisrt output" << std::endl;

      std::vector<CompactionEvent* > compaction_events = log_parser_.getCompactionEvent();
      std::vector<FlushEvent* > flush_events = log_parser_.getFlushEvent();
      for (size_t i = 0; i < compaction_events.size();i++){
	for (size_t j = i + 1; j < compaction_events.size();j++){
	  if( i != j
	      &&
	      compaction_events[i]->base_version_id_ == compaction_events[j]->base_version_id_
	      &&
	      compaction_events[i]->compaction_type_ != compaction_events[j]->compaction_type_
	      )

	    {
	      //std::cout << "i : " << i << "j : " << j << " i_base_ : "<<  compaction_events[i]->base_version_id_ << " j_base_ :" << compaction_events[j]->base_version_id_;
	      time_t start_time = std::min(compaction_events[i]->GetEventTime()*1000+compaction_events[i]->event_usec_/1000,
					   compaction_events[j]->GetEventTime()*1000+compaction_events[j]->event_usec_/1000);
	      time_t end_time   = std::max(compaction_events[i]->GetEventTime()*1000+compaction_events[i]->event_usec_/1000,
					   compaction_events[j]->GetEventTime()*1000+compaction_events[j]->event_usec_/1000);

	      int layer_id = std::max(compaction_events[i]->base_layer_, compaction_events[j]->base_layer_);
	      //std::cout << "layer_id :" << layer_id << " i_bl : " << compaction_events[i]->base_layer_ << " j_bl : " << compaction_events[j]->base_layer_ << std::endl;
	      variables_js << PrintEventData(std::string("#FF0077"),  std::string("L")+std::to_string(layer_id), layer_id+1, start_time, end_time) << std::endl;
	      break;
	    }
	}
      }

      for (size_t i = 0; i < flush_events.size();i++){
	for (size_t j = i + 1; j < flush_events.size();j++){
	  if( i != j
	      &&
	      flush_events[i]->version_id_ == flush_events[j]->version_id_
	      &&
	      flush_events[i]->flush_type_ != flush_events[j]->flush_type_
	      )

	    {
	      //std::cout << "i : " << i << "j : " << j << " i_base_ : "<<  flush_events[i]->version_id_ << " j_base_ :" << flush_events[j]->version_id_ << std::endl;
	      time_t start_time = std::min(flush_events[i]->GetEventTime()*1000+flush_events[i]->event_usec_/1000,
					   flush_events[j]->GetEventTime()*1000+flush_events[j]->event_usec_/1000);
	      time_t end_time   = std::max(flush_events[i]->GetEventTime()*1000+flush_events[i]->event_usec_/1000,
					   flush_events[j]->GetEventTime()*1000+flush_events[j]->event_usec_/1000);
	      variables_js << PrintEventData(std::string("#FF0077"),  std::string("flush"), 0, start_time, end_time) << std::endl;
	      break;
	    }
	}
      }
      variables_js << "];"                << std::endl;

      std::cout << "Print End" << std::endl;
    };

    std::string PrintEventData(std::string color, std::string name, int x, time_t start_time, time_t end_time)
    {
      std::string datas;
      datas += std::string("[");
      datas += std::to_string(x);
      datas += std::string(",");
      datas += std::to_string(start_time);
      datas += std::string(",");
      datas += std::to_string(end_time);
      datas += std::string("],");

      std::stringstream ss;
      ss <<"{" <<
	"color : '" << color << "'," << 
	"name  : '" << name  << "'," <<
	"type  : 'columnrange'," <<
	"data  : ["<<
	datas <<
	"]},"; 
      return ss.str();
    };

  private:
    rocksdb::LOGParser log_parser_;
  };
}

int main(int argc, char** argv) {
  rocksdb::VisualizationGenerator vis_gen;
  vis_gen.PrintEventDatas();
}

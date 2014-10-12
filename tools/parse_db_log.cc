#include <iostream>
#include <fstream>
#include <string>
#include <time.h>
#include <stdio.h>
#include <math.h>
#include <rocksdb/Slice.h>
#include <vector>
#include <sstream> 
#include <algorithm>
#include <iterator>

namespace rocksdb
{
  enum EventLogType{
    kNoUseEvent,
    kCompactionEvent,
    kFlushEvent,
    kStatisticsEvent
  };

  class EventLog{
  private:
    time_t event_time_;
    
  public:
    EventLog(){};
    ~EventLog(){};
    void SetEventTime(time_t time){event_time_=time;};
    time_t GetEventTime(){return event_time_;};
    
    virtual bool Parse(std::string log_string){return false;};
    virtual bool Parse(std::vector<std::string> log_string){return false;};
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
	char file_input_ids_origin[4096], file_input_ids_high[4096];
	std::sscanf(log_string.c_str(), " Base version %d Base level %d, seek compaction:0, inputs: [%s], [%s]",
		    &base_version_id_, &base_layer_, file_input_ids_origin, file_input_ids_high);
	return true;
      }else if(log_string.find("Compacted to") != std::string::npos){
	compaction_type_ = kCompactionFinish;
	log_string = log_string.substr(log_string.find(":") + 1);
	char other[256];
	std::sscanf(log_string.c_str(), " Base version %d files%s", &base_version_id_, other);  
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
    FlushEvent():write_bytes_(-1){};
    ~FlushEvent(){};

    bool Parse(std::string log_string)
    {
      log_string = log_string.substr(log_string.find("]") + 2);
      if(log_string.find("Level-0 flush") != std::string::npos){
	if(log_string.find("started") != std::string::npos){
	  flush_type_ = kFlushCreate;
	  std::sscanf(log_string.c_str(), "Level-0 flush table #%d", &version_id_);
	  return true;
	}
	if(log_string.find("bytes") != std::string::npos){
	  flush_type_ = kFlushFinish;
	  std::sscanf(log_string.c_str(), " Level-0 flush table #%d: %ld bytes OK", &version_id_, &write_bytes_);
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
    long write_bytes_;

  private:
  };

  class StatisticsEvent: public EventLog
  {
  public:
    StatisticsEvent(){};
    ~StatisticsEvent(){};

    bool Parse(std::vector<std::string> log_strings)
    {
      for(size_t i = 0; i < log_strings.size(); i++){
	std::string log_string = log_strings[i];
	std::cout << log_string << std::endl;
	//Skip Unneeded Strings
	if(i < 4)
	  continue;

	//For Other Check the first words
	std::string tokens = log_string.substr(0, log_string.find(" "));
	if(!tokens.compare(std::string("Flush(GB):"))){
	  std::sscanf(log_string.c_str(), "Flush(GB): accumulative %f, interval %f",
		      &flush_infos_.accumulative, &flush_infos_.interval);
	  std::cout << "flush(gb)" << flush_infos_.accumulative << " : " << flush_infos_.interval << std::endl;
	}else if(!tokens.compare("Stalls(secs):")){
	  std::sscanf(log_string.c_str(), "Stalls(secs): %f level0_slowdown, %f level0_numfiles, %f memtable_compaction, %f leveln_slowdown_soft, %f leveln_slowdown_hard",
		      &stalls_secs_.level0_slowdown,&stalls_secs_.level0_numfiles,&stalls_secs_.memtable_compaction,&stalls_secs_.leveln_slowdown_soft,&stalls_secs_.leveln_slowdown_hard);
	  std::cout << "stalls(secs)" <<  stalls_secs_.level0_slowdown << " : " <<stalls_secs_.level0_numfiles << " : " <<stalls_secs_.memtable_compaction << " : " <<stalls_secs_.leveln_slowdown_soft << " : " <<stalls_secs_.leveln_slowdown_hard << std::endl;
	}else if(!tokens.compare("Stalls(count):")){
	  std::sscanf(log_string.c_str(), "Stalls(secs): %d level0_slowdown, %d level0_numfiles, %d memtable_compaction, %d leveln_slowdown_soft, %d leveln_slowdown_hard",
		      &stalls_count_.level0_slowdown,&stalls_count_.level0_numfiles,&stalls_count_.memtable_compaction,&stalls_count_.leveln_slowdown_soft,&stalls_count_.leveln_slowdown_hard);
	}else if(!tokens.compare("**")){
	}else if(!tokens.compare("Uptime(secs):")){
	  std::sscanf(log_string.c_str(), "Uptime(secs): %f total, %f interval",
		      &uptime_secs_.total, &uptime_secs_.interval);
	}else if(!tokens.compare("Cumulative")){
	  if(log_string.find("writes:") != std::string::npos){
	    std::sscanf(log_string.c_str(), "Cumulative writes: %d writes, %d batches, %f writes per batch, %f GB user ingest",
			&cumulative_writes_.writes, &cumulative_writes_.batches, &cumulative_writes_.writes_per_batch, &cumulative_writes_.gb_user_ingest);
	  }else if(log_string.find("WAL:") != std::string::npos){
	    std::sscanf(log_string.c_str(), "Cumulative WAL: %d writes, %d syncs, %f writes per sync, %f GB written",
			&cumulative_wal_.writes, &cumulative_wal_.syncs, &cumulative_wal_.writes_per_sync, &cumulative_wal_.gb_written);
	  }
	}else if(!tokens.compare("Interval:")){
	  if(log_string.find("writes:") != std::string::npos){
	    std::sscanf(log_string.c_str(), "Interval writes: %d writes, %d batches, %f writes per batch, %f MB user ingest",
			&interval_writes_.writes, &interval_writes_.batches, &interval_writes_.writes_per_batch, &interval_writes_.mb_user_ingest);
	  }else if(log_string.find("WAL:") != std::string::npos){
	    std::sscanf(log_string.c_str(), "Interval WAL: %d writes, %d syncs, %f writes per sync, %f MB written",
			&interval_wal_.writes, &interval_wal_.syncs, &interval_wal_.writes_per_sync, &interval_wal_.mb_written);
	  }
	}else{
	  if(log_string.length()){
	    struct compaction_infos tmp_compaction_infos;
	    std::sscanf(log_string.c_str(), "%s %s %d %f %f %f %f %f %f %f %f %f %f %d %d %d %d %d %d %f %f %d %f %d %d",
			tmp_compaction_infos.name,
			tmp_compaction_infos.files,
			&tmp_compaction_infos.size_mb,
			&tmp_compaction_infos.score,
			&tmp_compaction_infos.Read_gb,
			&tmp_compaction_infos.Rn_gb,
			&tmp_compaction_infos.Rnp1_gb,
			&tmp_compaction_infos.Write_gb,
			&tmp_compaction_infos.Wnew_gb,
			&tmp_compaction_infos.RW_Amp,
			&tmp_compaction_infos.W_Amp,
			&tmp_compaction_infos.Rd_mb_per_s,
			&tmp_compaction_infos.Wr_mb_per_s,
			&tmp_compaction_infos.Rn_cnt,
			&tmp_compaction_infos.Rnp1_cnt,
			&tmp_compaction_infos.Wnp1_cnt,
			&tmp_compaction_infos.Wnew_cnt,
			&tmp_compaction_infos.Comp_sec,
			&tmp_compaction_infos.Comp_cnt,
			&tmp_compaction_infos.Avg_ms,
			&tmp_compaction_infos.Stall_sec,
			&tmp_compaction_infos.Stall_cnt,
			&tmp_compaction_infos.Avg_ms,
			&tmp_compaction_infos.RecordIn,
			&tmp_compaction_infos.RecordDrop);
	    compaction_infos_vector_.push_back(tmp_compaction_infos);
	  }
	}
      }
      return true;
    }

    struct compaction_infos{
      char name[256];
      char files[256];
      int size_mb;
      float score;
      float Read_gb;
      float Rn_gb;
      float Rnp1_gb;
      float Write_gb;
      float Wnew_gb;
      float RW_Amp;
      float W_Amp;
      float Rd_mb_per_s;
      float Wr_mb_per_s;
      int Rn_cnt;
      int Rnp1_cnt;
      int Wnp1_cnt;
      int Wnew_cnt;
      int Comp_sec;
      int Comp_cnt;
      float Avg_sec;
      float Stall_sec;
      int Stall_cnt;
      float Avg_ms;
      int RecordIn;
      int RecordDrop;
    };

    struct flush_infos{
      float accumulative;
      float interval;
    };

    struct stalls_secs{
      float level0_slowdown;
      float level0_numfiles;
      float memtable_compaction;
      float leveln_slowdown_soft;
      float leveln_slowdown_hard;
    };

    struct stalls_count{
      int level0_slowdown;
      int level0_numfiles;
      int memtable_compaction;
      int leveln_slowdown_soft;
      int leveln_slowdown_hard;
    };

    struct uptime_secs{
      float total;
      float interval;
    };
    
    struct cumulative_writes{
      int writes;
      int batches;
      float writes_per_batch;
      float gb_user_ingest;
    };

    struct cumulative_wal{
      int writes;
      int syncs;
      float writes_per_sync;
      float gb_written;
    };

    struct interval_writes{
      int writes;
      int batches;
      float writes_per_batch;
      float mb_user_ingest;
    };

    struct interval_wal{
      int writes;
      int syncs;
      float writes_per_sync;
      float mb_written;
    };

    flush_infos flush_infos_;
    stalls_secs stalls_secs_;
    stalls_count stalls_count_;
    uptime_secs uptime_secs_;
    cumulative_writes cumulative_writes_;
    cumulative_wal cumulative_wal_;
    interval_writes interval_writes_;
    interval_wal interval_wal_;
    std::vector<compaction_infos> compaction_infos_vector_;
  };

};//namespace rocksdb


namespace rocksdb
{
  class LOGParser{
  private:
    std::vector<CompactionEvent*> parsed_compaction_event_logs_;
    std::vector<FlushEvent*> parsed_flush_event_logs_;
    std::vector<StatisticsEvent*> parsed_statistics_event_logs_;
    
  public:
    std::vector<CompactionEvent*> getCompactionEvent(){return parsed_compaction_event_logs_;};
    std::vector<FlushEvent*> getFlushEvent(){return parsed_flush_event_logs_;};
    LOGParser(){};
    ~LOGParser(){};

    void Parse(std::string filename)
    {
      std::cout << "Parse start" << std::endl;

      std::ifstream LOGfile(filename);
      std::string line;
      int counter = 0;
      std::vector<std::string> string_concatenate_buffer;
      bool concatenate_span = false;
      EventLogType concatenate_log_type = kNoUseEvent;
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

	  //If just Concatenate
	  if(concatenate_span){
	    string_concatenate_buffer.push_back(line);
	    if(CheckConcatenateEnd(line)){
	      concatenate_span = false;
	      
	      if(concatenate_log_type == kStatisticsEvent){
		StatisticsEvent* event_log = new StatisticsEvent();
		if(event_log->Parse(string_concatenate_buffer))
		  parsed_statistics_event_logs_.push_back(event_log);
	      }
	      concatenate_log_type = kNoUseEvent;
	      string_concatenate_buffer.clear();
	      continue;
	    }else{
	      continue;
	    }
	  }

	  if(ret == EOF)
	    std::cout << "ERROR" << std::endl;
	  else if(ret == 0){}
	  else{
	    t.tm_year -= 1900;
	    t.tm_mon  -= 1;
	    time_t unix_timestamp = mktime(&t);

	    std::string thread_string(thread_id);
	    int cutoff_offset = 28 + thread_string.length();
	    std::string log_string = line.substr(cutoff_offset).c_str();

	    //Check concatenate timing string
	    EventLogType need_concatenate = CheckNeedConcatenate(log_string);
	    if(need_concatenate != kNoUseEvent){
	      concatenate_span = true;
	      string_concatenate_buffer.push_back(log_string);
	      concatenate_log_type = need_concatenate;
	      continue;
	    }

	    //Not need  to concatenate
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
	  counter++;
	}
      std::cout << "Total line : " << counter <<  " Parse End" << std::endl; 
   }

    bool CheckConcatenateEnd(std::string log_string){
      if(log_string.find("DUMPING STATS END") != std::string::npos){
	return true;
      }
      return false;
    }

    EventLogType CheckNeedConcatenate(std::string log_string){
      if(log_string.find("DUMPING STATS") != std::string::npos){
	return kStatisticsEvent;
      }
      return kNoUseEvent;
    }

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
    VisualizationGenerator(std::string filename)
    {
      log_parser_.Parse(filename);
    };
    ~VisualizationGenerator(){};

    void PrintEventDatas()
    {
      std::cout << "Print " << std::endl;

      std::vector<std::string> colors;
      colors.push_back(std::string("#0066FF"));
      colors.push_back(std::string("#6600FF"));
      colors.push_back(std::string("#FF0066"));
      colors.push_back(std::string("#FF66FF"));
      colors.push_back(std::string("#66FFFF"));
      colors.push_back(std::string("#FFFF66"));

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
	      variables_js << PrintEventData(colors[layer_id],
					     std::string("L")+std::to_string(layer_id)+std::string("#")+std::to_string(compaction_events[i]->base_version_id_),
					     layer_id+1,
					     start_time, end_time) << std::endl;
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
	      long write_byte = std::max(flush_events[i]->write_bytes_,flush_events[j]->write_bytes_);
	      variables_js << PrintEventData(std::string("#FF0077"),  std::string("flush#")+std::to_string(flush_events[i]->version_id_)+std::string("(Wb:")+std::to_string(write_byte)+std::string(")"), 0, start_time, end_time) << std::endl;
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
  if (argc < 2){
    std::cout << "Please pass the file path" << std::endl;
    return -1;
  }

  rocksdb::VisualizationGenerator vis_gen(argv[1]);
  vis_gen.PrintEventDatas();
}

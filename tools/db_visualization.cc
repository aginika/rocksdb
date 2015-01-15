#include "utilities/parse_db_log/parse_db_log.h"

namespace rocksdb
{
  class VisualizationGenerator{
  public:
    VisualizationGenerator(std::string filename, int id)
    {
      id_ = id;
      log_parser_.Parse(filename);
    };
    ~VisualizationGenerator(){};

    void PrintFileNums()
    {
      std::cout << "PrintFileNums " << std::endl;

      std::ofstream variables_js("variables.js", std::ios_base::app | std::ios_base::out);
      std::vector<CompactionEvent* > compaction_events = log_parser_.getCompactionEvent();
      std::map<int, std::vector<std::pair<long, int> >* > layer_file_nums;

      for (int i = 0; i < 7; i++)
	layer_file_nums[i] = new std::vector<std::pair<long, int>>();
      for(size_t i = 0; i < compaction_events.size(); i++){
	if(compaction_events[i]->compaction_type_ == CompactionEvent::kCompactionFinish){
	  for (int j = 0; j < 7; j++)
	    layer_file_nums[j]->push_back(std::pair<long,int>(compaction_events[i]->GetEventTime(), compaction_events[i]->l0_file_num_));
	}
      }

      int layer_id = 0;
      for (std::map<int, std::vector< std::pair<long, int> >* >::iterator itpairstri = layer_file_nums.begin();
	   itpairstri != layer_file_nums.end();
	   itpairstri++) 
	{
	  std::vector< std::pair<long, int> > layer_values  = *(itpairstri->second);
	  variables_js << "var file_nums_"<< std::string("L")+std::to_string(layer_id) << std::string("_") << id_ << " = " << std::endl;
	  variables_js << "{name:'"<< std::string("L")+std::to_string(layer_id) <<"',"                  << std::endl;
	  variables_js << "data: [" <<  std::endl;
	  for (size_t i = 0; i < layer_values.size();i++){
	    long js_unixtime = layer_values[i].first;
	    float compaction_wb = layer_values[i].second;
	    variables_js << "[" << js_unixtime << "," << compaction_wb << "]," << std::endl;
	  }
	  variables_js << "]};" << std::endl;
	  layer_id++;
	}
    }

    void PrintLineChartsOfWriteBytes()
    {
      std::cout << "PrintLineChartsOfWriteBytes " << std::endl;

      std::vector<CompactionEvent* > compaction_events = log_parser_.getCompactionEvent();
      //divide with layer_id
      int max_layer = -1;
      std::map<int, std::vector<std::pair<long, float> >* > compaction_events_with_id;
      for(size_t i = 0; i < compaction_events.size(); i++){
	if(compaction_events[i]->compaction_type_ == CompactionEvent::kCompactionFinish){
	  int target_layer_id = compaction_events[i]->base_layer_;
	  if (target_layer_id > max_layer){
	    for (int add_layer_index = max_layer + 1 ; add_layer_index <= target_layer_id ; add_layer_index++){
	      compaction_events_with_id[add_layer_index] = new std::vector<std::pair<long, float> >();
	      max_layer = add_layer_index;
	    }
	  }
	  std::pair<long,float> compaction_event(compaction_events[i]->GetEventTime()*1000+compaction_events[i]->event_usec_/1000,
						 compaction_events[i]->write_mb_per_sec_);
	
	  compaction_events_with_id[target_layer_id]->push_back(compaction_event);
	}
      }
      PrintLineChartsOfValues(std::string("write"), max_layer, compaction_events_with_id);
    };

    void PrintLineChartsOfValues(std::string keyword, int max_layer, std::map<int, std::vector<std::pair<long, float> >* > datas)
    {
      std::ofstream variables_js("variables.js", std::ios_base::app | std::ios_base::out);
      std::cout << "Compaction_events_with_id("<< keyword <<") :" << std::endl;
      std::cout << "    size     : "<< datas.size() << std::endl;
      std::cout << "    max_layer: "<< max_layer << std::endl;
      
      for (std::map<int, std::vector< std::pair<long, float> >* >::iterator itpairstri = datas.begin();
	   itpairstri != datas.end();
	   itpairstri++) 
	{
	  int layer_id = itpairstri->first ;
	  std::vector< std::pair<long,float> > layer_values  = *(itpairstri->second);

	  variables_js << "var "<< keyword <<"_datas_"<< std::string("L")+std::to_string(layer_id) << std::string("_") << id_ <<" = " << std::endl;
	  variables_js << "{name:'"<< std::string("L")+std::to_string(layer_id-1) << "->" <<std::string("L")+std::to_string(layer_id) <<"',"                  << std::endl;
	  variables_js << "data: [" <<  std::endl;
	  for (size_t i = 0; i < layer_values.size();i++){
	  	long js_unixtime = layer_values[i].first;
	  	float compaction_wb = layer_values[i].second;
	  	variables_js << "[" << js_unixtime << "," << compaction_wb << "]," << std::endl;
	  }
	  variables_js << "]};" << std::endl;
	}
    }

    void PrintLineChartsOfReadBytes()
    {
      std::cout << "PrintLineChartsOfReadBytes " << std::endl;

      std::ofstream variables_js("variables.js", std::ios_base::app | std::ios_base::out);
      std::vector<CompactionEvent* > compaction_events = log_parser_.getCompactionEvent();
      //divide with layer_id
      int max_layer = -1;
      std::map<int, std::vector<std::pair<long, float> >* > compaction_events_with_id;
      for(size_t i = 0; i < compaction_events.size(); i++){
	if(compaction_events[i]->compaction_type_ == CompactionEvent::kCompactionFinish){
	  int target_layer_id = compaction_events[i]->base_layer_;
	  if (target_layer_id > max_layer){
	    for (int add_layer_index = max_layer + 1 ; add_layer_index <= target_layer_id ; add_layer_index++){
	      compaction_events_with_id[add_layer_index] = new std::vector<std::pair<long, float> >();
	      max_layer = add_layer_index;
	    }
	  }
	  std::pair<long,float> compaction_event(compaction_events[i]->GetEventTime()*1000+compaction_events[i]->event_usec_/1000,
						 compaction_events[i]->read_mb_per_sec_);
	
	  compaction_events_with_id[target_layer_id]->push_back(compaction_event);
	}
      }
      PrintLineChartsOfValues(std::string("read"), max_layer, compaction_events_with_id);
    };

    void PrintLineChartsOfInputFileNMB()
    {
      std::cout << "PrintLineChartsOfInputFileNMB " << std::endl;

      std::ofstream variables_js("variables.js", std::ios_base::app | std::ios_base::out);
      std::vector<CompactionEvent* > compaction_events = log_parser_.getCompactionEvent();
      //divide with layer_id
      int max_layer = -1;
      std::map<int, std::vector<std::pair<long, float> >* > compaction_events_with_id;
      for(size_t i = 0; i < compaction_events.size(); i++){
	if(compaction_events[i]->compaction_type_ == CompactionEvent::kCompactionFinish){
	  int target_layer_id = compaction_events[i]->base_layer_;
	  if (target_layer_id > max_layer){
	    for (int add_layer_index = max_layer + 1 ; add_layer_index <= target_layer_id ; add_layer_index++){
	      compaction_events_with_id[add_layer_index] = new std::vector<std::pair<long, float> >();
	      max_layer = add_layer_index;
	    }
	  }
	  std::pair<long,float> compaction_event(compaction_events[i]->GetEventTime()*1000+compaction_events[i]->event_usec_/1000,
						 compaction_events[i]->input_file_n_mb_);
	
	  compaction_events_with_id[target_layer_id]->push_back(compaction_event);
	}
      }
      PrintLineChartsOfValues(std::string("input_file_n_mb"), max_layer, compaction_events_with_id);
    };

     void PrintLineChartsOfInputFileNp1MB()
    {
      std::cout << "PrintLineChartsOfInputFileNp1MB " << std::endl;

      std::ofstream variables_js("variables.js", std::ios_base::app | std::ios_base::out);
      std::vector<CompactionEvent* > compaction_events = log_parser_.getCompactionEvent();
      //divide with layer_id
      int max_layer = -1;
      std::map<int, std::vector<std::pair<long, float> >* > compaction_events_with_id;
      for(size_t i = 0; i < compaction_events.size(); i++){
	if(compaction_events[i]->compaction_type_ == CompactionEvent::kCompactionFinish){
	  int target_layer_id = compaction_events[i]->base_layer_;
	  if (target_layer_id > max_layer){
	    for (int add_layer_index = max_layer + 1 ; add_layer_index <= target_layer_id ; add_layer_index++){
	      compaction_events_with_id[add_layer_index] = new std::vector<std::pair<long, float> >();
	      max_layer = add_layer_index;
	    }
	  }
	  std::pair<long,float> compaction_event(
						 compaction_events[i]->GetEventTime()*1000+compaction_events[i]->event_usec_/1000,
						 compaction_events[i]->input_file_np1_mb_);
	
	  compaction_events_with_id[target_layer_id]->push_back(compaction_event);
	}
      }
      PrintLineChartsOfValues(std::string("input_file_np1_mb"), max_layer, compaction_events_with_id);
    };

    void PrintLineChartsOfOutputFileMB()
    {
      std::cout << "PrintLineChartsOfOutputFIleMB " << std::endl;

      std::ofstream variables_js("variables.js", std::ios_base::app | std::ios_base::out);
      std::vector<CompactionEvent* > compaction_events = log_parser_.getCompactionEvent();
      //divide with layer_id
      int max_layer = -1;
      std::map<int, std::vector<std::pair<long, float> >* > compaction_events_with_id;
      for(size_t i = 0; i < compaction_events.size(); i++){
	if(compaction_events[i]->compaction_type_ == CompactionEvent::kCompactionFinish){
	  int target_layer_id = compaction_events[i]->base_layer_;
	  if (target_layer_id > max_layer){
	    for (int add_layer_index = max_layer + 1 ; add_layer_index <= target_layer_id ; add_layer_index++){
	      compaction_events_with_id[add_layer_index] = new std::vector<std::pair<long, float> >();
	      max_layer = add_layer_index;
	    }
	  }
	  std::pair<long,float> compaction_event(
						 compaction_events[i]->GetEventTime()*1000+compaction_events[i]->event_usec_/1000,
						 compaction_events[i]->output_file_mb_);
	
	  compaction_events_with_id[target_layer_id]->push_back(compaction_event);
	}
      }
      PrintLineChartsOfValues(std::string("output_file_mb"), max_layer, compaction_events_with_id);
    };

    void PrintLineChartsOfReadAM()
    {
      std::cout << "PrintLineChartsOfReadAM " << std::endl;

      std::ofstream variables_js("variables.js", std::ios_base::app | std::ios_base::out);
      std::vector<CompactionEvent* > compaction_events = log_parser_.getCompactionEvent();
      //divide with layer_id
      int max_layer = -1;
      std::map<int, std::vector<std::pair<long, float> >* > compaction_events_with_id;
      for(size_t i = 0; i < compaction_events.size(); i++){
	if(compaction_events[i]->compaction_type_ == CompactionEvent::kCompactionFinish){
	  int target_layer_id = compaction_events[i]->base_layer_;
	  if (target_layer_id > max_layer){
	    for (int add_layer_index = max_layer + 1 ; add_layer_index <= target_layer_id ; add_layer_index++){
	      compaction_events_with_id[add_layer_index] = new std::vector<std::pair<long, float> >();
	      max_layer = add_layer_index;
	    }
	  }
	  std::pair<long,float> compaction_event(
						 compaction_events[i]->GetEventTime()*1000+compaction_events[i]->event_usec_/1000,
						 compaction_events[i]->read_am_);
	
	  compaction_events_with_id[target_layer_id]->push_back(compaction_event);
	}
      }
      PrintLineChartsOfValues(std::string("read_am"), max_layer, compaction_events_with_id);
    };

    void PrintLineChartsOfWriteAM()
    {
      std::cout << "PrintLineChartsOfWriteAM " << std::endl;

      std::ofstream variables_js("variables.js", std::ios_base::app | std::ios_base::out);
      std::vector<CompactionEvent* > compaction_events = log_parser_.getCompactionEvent();
      //divide with layer_id
      int max_layer = -1;
      std::map<int, std::vector<std::pair<long, float> >* > compaction_events_with_id;
      for(size_t i = 0; i < compaction_events.size(); i++){
	if(compaction_events[i]->compaction_type_ == CompactionEvent::kCompactionFinish){
	  int target_layer_id = compaction_events[i]->base_layer_;
	  if (target_layer_id > max_layer){
	    for (int add_layer_index = max_layer + 1 ; add_layer_index <= target_layer_id ; add_layer_index++){
	      compaction_events_with_id[add_layer_index] = new std::vector<std::pair<long, float> >();
	      max_layer = add_layer_index;
	    }
	  }
	  std::pair<long,float> compaction_event(
						 compaction_events[i]->GetEventTime()*1000+compaction_events[i]->event_usec_/1000,
						 compaction_events[i]->write_am_);
	
	  compaction_events_with_id[target_layer_id]->push_back(compaction_event);
	}
      }
      PrintLineChartsOfValues(std::string("write_am"), max_layer, compaction_events_with_id);
    };

    void PrintOptionDatas()
    {
      OptionsEvent* option_events = (log_parser_.getOptionsEvent())[0];
      std::ofstream variables_js("variables.js", std::ios_base::app | std::ios_base::out);
      variables_js << "var option_datas"<< std::string("_") << id_ <<" = " << std::endl;
      variables_js << "["                  << std::endl;
      std::cout << "OptionData output" << std::endl;
      for(size_t i = 0; i < option_events->options_vector_.size(); i++){
	variables_js << "'" << option_events->options_vector_[i] << "',"  << std::endl;
      }
      variables_js << "];" << std::endl;
      std::cout << "OptionData output END" << std::endl;
    }

    void PrintLatestDatabaseStatus()
    {
      std::cout << "LatestDatabaseStatus output" << std::endl;
      StatisticsEvent* latest_statistics_event = log_parser_.getStatisticsEvent().back();
      std::ofstream variables_js("variables.js", std::ios_base::app | std::ios_base::out);
      variables_js << "var latest_statistics_total_datas" << std::string("_") << id_ <<" = " << std::endl;
      variables_js << "["                  << std::endl;
      variables_js << "'" << latest_statistics_event->uptime_secs_.total << "',"  << std::endl;
      variables_js << "'" << latest_statistics_event->cumulative_writes_.writes << "',"  << std::endl;
      variables_js << "'" << latest_statistics_event->cumulative_writes_.batches << "',"  << std::endl;
      variables_js << "'" << latest_statistics_event->cumulative_writes_.gb_user_ingest << "',"  << std::endl;
      variables_js << "'" << latest_statistics_event->flush_infos_.accumulative << "',"  << std::endl;
      variables_js << "];" << std::endl;

      variables_js << "var latest_statistics_compaction_datas" << std::string("_") << id_ <<" = " << std::endl;
      variables_js << "["                  << std::endl;
      std::vector<StatisticsEvent::compaction_infos> compaction_info_vector = latest_statistics_event->compaction_infos_vector_;
      for(size_t i = 0; i < compaction_info_vector.size();i++)
	{
	  variables_js << "{" << compaction_info_vector[i].name << ":" << std::endl;
	  variables_js << "["<< std::endl;
	  variables_js << "'" << compaction_info_vector[i].files << "',"  << std::endl;
	  variables_js << "'" << compaction_info_vector[i].size_mb << "',"  << std::endl;
	  variables_js << "'" << compaction_info_vector[i].Read_gb << "',"  << std::endl;
	  variables_js << "'" << compaction_info_vector[i].Rn_gb << "',"  << std::endl;
	  variables_js << "'" << compaction_info_vector[i].Rnp1_gb << "',"  << std::endl;
	  variables_js << "'" << compaction_info_vector[i].Write_gb << "',"  << std::endl;
	  variables_js << "'" << compaction_info_vector[i].Wnew_gb << "',"  << std::endl;
	  variables_js << "'" << compaction_info_vector[i].RW_Amp << "',"  << std::endl;
	  variables_js << "'" << compaction_info_vector[i].W_Amp << "',"  << std::endl;
	  variables_js << "'" << compaction_info_vector[i].Rd_mb_per_s << "',"  << std::endl;
	  variables_js << "'" << compaction_info_vector[i].Wr_mb_per_s << "',"  << std::endl;
	  variables_js << "'" << compaction_info_vector[i].Rn_cnt << "',"  << std::endl;
	  variables_js << "'" << compaction_info_vector[i].Rnp1_cnt << "',"  << std::endl;
	  variables_js << "'" << compaction_info_vector[i].Wnp1_cnt << "',"  << std::endl;
	  variables_js << "'" << compaction_info_vector[i].Wnew_cnt << "',"  << std::endl;
	  variables_js << "'" << compaction_info_vector[i].Comp_sec << "',"  << std::endl;
	  variables_js << "'" << compaction_info_vector[i].Comp_cnt << "',"  << std::endl;
	  variables_js << "'" << compaction_info_vector[i].Avg_sec << "',"  << std::endl;
	  variables_js << "'" << compaction_info_vector[i].Stall_sec << "',"  << std::endl;
	  variables_js << "'" << compaction_info_vector[i].Stall_cnt << "',"  << std::endl;
	  variables_js << "'" << compaction_info_vector[i].Avg_ms << "',"  << std::endl;
	  variables_js << "'" << compaction_info_vector[i].RecordIn << "',"  << std::endl;
	  variables_js << "'" << compaction_info_vector[i].RecordDrop << "',"  << std::endl;
	  variables_js << "]},"<< std::endl;
	}

      variables_js << "];" << std::endl;      

      std::cout << "LatestDatabaseStatus output END" << std::endl;
    }

    void PrintEventDatas()
    {
      std::cout << "PrintEventDatas " << std::endl;

      std::vector<std::string> colors;
      colors.push_back(std::string("#0066FF"));
      colors.push_back(std::string("#6600FF"));
      colors.push_back(std::string("#FF0066"));
      colors.push_back(std::string("#FF66FF"));
      colors.push_back(std::string("#66FFFF"));
      colors.push_back(std::string("#FFFF66"));

      std::ofstream variables_js("variables.js", std::ios_base::app | std::ios_base::out);
      variables_js << "var event_datas" << std::string("_") << id_ <<" = " << std::endl;
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
    int id_;
  };
}//namespace rocksdb

int main(int argc, char** argv) {
  if (argc < 2){
    std::cout << "Please pass the file path" << std::endl;
    return -1;
  }
  std::vector<std::string> names;
  for(int i = 0; i < argc - 1; i++){
    rocksdb::VisualizationGenerator vis_gen(argv[i + 1], i);
    vis_gen.PrintFileNums();
    vis_gen.PrintLineChartsOfWriteBytes();
    vis_gen.PrintLineChartsOfReadBytes();
    vis_gen.PrintLineChartsOfInputFileNMB();
    vis_gen.PrintLineChartsOfInputFileNp1MB();
    vis_gen.PrintLineChartsOfOutputFileMB();
    vis_gen.PrintLineChartsOfReadAM();
    vis_gen.PrintLineChartsOfWriteAM();
    vis_gen.PrintOptionDatas();
    vis_gen.PrintLatestDatabaseStatus();
    vis_gen.PrintEventDatas();
  }
}

CREATE TRIGGER SUT_SBS_VIEW_SCRIPT_BRIU
BEFORE INSERT OR UPDATE ON SBS_UTIL.SBS_VIEW_SCRIPT
    FOR EACH ROW EXECUTE FUNCTION SBS_UTIL.SUT_TRG_FNC_SBS_VIEW_SCRIPT_BRIU();
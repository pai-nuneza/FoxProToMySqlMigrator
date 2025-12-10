using System.IO;
using System.Text.Json;
using FoxProToMySqlMigrator.Models;

namespace FoxProToMySqlMigrator.Services
{
    internal class CheckpointService
    {
        private readonly string _checkpointFilePath;

        public CheckpointService(string checkpointFilePath)
        {
            _checkpointFilePath = checkpointFilePath;
        }

        public async Task<MigrationCheckpoint?> LoadCheckpointAsync(string foxProFolder, string targetDatabase)
        {
            try
            {
                if (File.Exists(_checkpointFilePath))
                {
                    var json = await File.ReadAllTextAsync(_checkpointFilePath);
                    var checkpoint = JsonSerializer.Deserialize<MigrationCheckpoint>(json);
                    
                    // Validate checkpoint matches current migration
                    if (checkpoint != null && 
                        checkpoint.FoxProFolder == foxProFolder && 
                        checkpoint.TargetDatabase == targetDatabase &&
                        !checkpoint.IsCompleted)
                    {
                        return checkpoint;
                    }
                }
            }
            catch
            {
                // If checkpoint can't be loaded, return null
            }

            return null;
        }

        public async Task SaveCheckpointAsync(MigrationCheckpoint checkpoint)
        {
            try
            {
                var json = JsonSerializer.Serialize(checkpoint, new JsonSerializerOptions 
                { 
                    WriteIndented = true 
                });
                await File.WriteAllTextAsync(_checkpointFilePath, json);
            }
            catch
            {
                // If checkpoint can't be saved, continue migration
            }
        }

        public void DeleteCheckpoint()
        {
            try
            {
                if (File.Exists(_checkpointFilePath))
                {
                    File.Delete(_checkpointFilePath);
                }
            }
            catch
            {
                // Ignore errors
            }
        }
    }
}

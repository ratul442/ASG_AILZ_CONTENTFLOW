import { useState } from "react";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { HelpCircle } from "lucide-react";
import { toast } from "sonner";
import type { Pipeline, CreateVaultRequest } from "@/types/components";

interface CreateVaultDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onCreateVault: (vault: CreateVaultRequest) => void;
  pipelines: Pipeline[];
  isLoading?: boolean;
}

export const CreateVaultDialog = ({
  open,
  onOpenChange,
  onCreateVault,
  pipelines,
  isLoading = false,
}: CreateVaultDialogProps) => {
  const [formData, setFormData] = useState<CreateVaultRequest>({
    name: "",
    description: "",
    pipeline_id: "",
    save_execution_output: false,
    enabled: true,
    tags: [],
  });
  const [tagInput, setTagInput] = useState("");

  const handleSubmit = () => {
    if (!formData.name.trim()) {
      toast.error("Please enter a vault name");
      return;
    }
    if (!formData.pipeline_id) {
      toast.error("Please select a content pipeline");
      return;
    }

    onCreateVault(formData);
    setFormData({
      name: "",
      description: "",
      pipeline_id: "",
      save_execution_output: false,
      enabled: true,
      tags: [],
    });
    setTagInput("");
  };

  const addTag = () => {
    if (tagInput.trim() && !(formData.tags || []).includes(tagInput.trim())) {
      setFormData({
        ...formData,
        tags: [...(formData.tags || []), tagInput.trim()],
      });
      setTagInput("");
    }
  };

  const removeTag = (tag: string) => {
    setFormData({
      ...formData,
      tags: (formData.tags || []).filter((t) => t !== tag),
    });
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <DialogTitle>Create New Vault</DialogTitle>
          <DialogDescription>
            Create a new content vault and associate it with a content pipeline
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4 py-4">
          <div className="space-y-2">
            <Label htmlFor="name">Vault Name *</Label>
            <Input
              id="name"
              value={formData.name}
              onChange={(e) => setFormData({ ...formData, name: e.target.value })}
              placeholder="e.g., AI Research Vault"
            />
          </div>

          <div className="space-y-2">
            <Label htmlFor="description">Description</Label>
            <Textarea
              id="description"
              value={formData.description}
              onChange={(e) => setFormData({ ...formData, description: e.target.value })}
              placeholder="Describe the purpose of this vault..."
              rows={3}
            />
          </div>

          <div className="space-y-2">
            <Label htmlFor="pipeline">Content Pipeline *</Label>
            <Select
              value={formData.pipeline_id}
              onValueChange={(value) => setFormData({ ...formData, pipeline_id: value })}
            >
              <SelectTrigger id="pipeline">
                <SelectValue placeholder="Select a pipeline" />
              </SelectTrigger>
              <SelectContent>
                {pipelines.map((pipeline) => (
                  <SelectItem key={pipeline.id} value={pipeline.id}>
                    {pipeline.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <div className="space-y-4">
            <div className="flex items-center space-x-2">
              <Checkbox
                id="enabled"
                checked={formData.enabled}
                onCheckedChange={(checked) =>
                  setFormData({ ...formData, enabled: checked === true })
                }
              />
              <Label htmlFor="enabled" className="cursor-pointer flex items-center gap-2">
                Enabled
                <TooltipProvider>
                  <Tooltip>
                    <TooltipTrigger asChild>
                      <HelpCircle className="w-4 h-4 text-muted-foreground" />
                    </TooltipTrigger>
                    <TooltipContent>
                      <p>When enabled, this vault will be active and available for use</p>
                    </TooltipContent>
                  </Tooltip>
                </TooltipProvider>
              </Label>
            </div>

            <div className="flex items-center space-x-2">
              <Checkbox
                id="save_execution_output"
                checked={formData.save_execution_output}
                onCheckedChange={(checked) =>
                  setFormData({ ...formData, save_execution_output: checked === true })
                }
              />
              <Label htmlFor="save_execution_output" className="cursor-pointer flex items-center gap-2">
                Save Execution Output
                <TooltipProvider>
                  <Tooltip>
                    <TooltipTrigger asChild>
                      <HelpCircle className="w-4 h-4 text-muted-foreground" />
                    </TooltipTrigger>
                    <TooltipContent>
                      <p>When enabled, pipeline execution output will be saved to Cosmos DB</p>
                    </TooltipContent>
                  </Tooltip>
                </TooltipProvider>
              </Label>
            </div>
          </div>

          <div className="space-y-2">
            <Label htmlFor="tags">Tags</Label>
            <div className="flex gap-2">
              <Input
                id="tags"
                value={tagInput}
                onChange={(e) => setTagInput(e.target.value)}
                onKeyPress={(e) => e.key === "Enter" && (e.preventDefault(), addTag())}
                placeholder="Add tags..."
              />
              <Button type="button" variant="outline" onClick={addTag}>
                Add
              </Button>
            </div>
            {(formData.tags && formData.tags.length > 0) && (
              <div className="flex flex-wrap gap-2 mt-2">
                {formData.tags.map((tag) => (
                  <div
                    key={tag}
                    className="bg-secondary text-secondary-foreground px-3 py-1 rounded-full text-sm flex items-center gap-2"
                  >
                    {tag}
                    <button
                      onClick={() => removeTag(tag)}
                      className="hover:text-destructive"
                    >
                      ×
                    </button>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)} disabled={isLoading}>
            Cancel
          </Button>
          <Button onClick={handleSubmit} className="bg-gradient-secondary" disabled={isLoading}>
            {isLoading ? "Creating..." : "Create Vault"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
};
